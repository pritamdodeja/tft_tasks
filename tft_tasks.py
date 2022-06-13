#!/usr/bin/env python
# coding: utf-8
# {{{ Imports
# Goal: Implement taxicab fare prediction with a tft pipeline with safety
# checks and a simpler flow.
import tensorflow_metadata
from typing import List, Dict
from dataclasses import dataclass, field
import argparse
import shutil
import glob
from tfx_bsl.public import tfxio
from tfx_bsl.coders.example_coder import RecordBatchToExamples
import tensorflow_transform.beam as tft_beam
import tensorflow_transform as tft
import apache_beam as beam
import tensorflow_addons as tfa
from tensorflow.keras import layers
import tensorflow as tf
from tensorflow_transform.tf_metadata import schema_utils
import tempfile
import logging
import os
import pickle
import collections
from trace_path import TracePath
from functools import partial
MyTracePath = TracePath(instrument=True, name="MyTracePath")
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'  # or any {'0', '1', '2'}
# set TF error log verbosity
logger = logging.getLogger("tensorflow").setLevel(logging.INFO)
# print(tf.version.VERSION)
# }}}
# {{{ TaxiCabData Data Class


@dataclass(frozen=True)
class TaxiCabData:
    NBUCKETS: int
    LABEL_COLUMN: str
    TEST_FILE_PATH: str
    TRAIN_FILE_PATH: str
    PREFIX_STRING: str
    DEFAULT_DTYPES: List[tf.DType]
    ALL_DTYPES: List[tf.DType]
    CSV_COLUMNS: List[str] = field(default_factory=list)
    STRING_COLS: List[str] = field(default_factory=list)
    NUMERIC_COLS: List[str] = field(default_factory=list)
    DEFAULTS: List = field(default_factory=list)
    RAW_DATA_DICTIONARY: Dict[str, tf.DType] = field(init=False)
    RAW_DATA_FEATURE_SPEC: Dict[str, tf.DType] = field(init=False)
    _SCHEMA: tensorflow_metadata.proto.v0.schema_pb2.Schema = field(init=False)
    BATCH_SIZE: int = 8

    def __post_init__(self):
        tempdict = {
            key: value for key, value in zip(
                self.CSV_COLUMNS, self.ALL_DTYPES
                )
            }
        object.__setattr__(self, "RAW_DATA_DICTIONARY", tempdict)
        tempdict = {
            raw_feature_name: tf.io.FixedLenFeature(
                shape=(1,),
                dtype=self.RAW_DATA_DICTIONARY[raw_feature_name])
            for raw_feature_name in self.CSV_COLUMNS}
        object.__setattr__(self, "RAW_DATA_FEATURE_SPEC", tempdict)
        _SCHEMA = schema_utils.schema_from_feature_spec(
            self.RAW_DATA_FEATURE_SPEC)
        object.__setattr__(self, "_SCHEMA", _SCHEMA)

# }}}
# {{{ Custom default dict class


class custom_default_dict(collections.defaultdict):
    def return_none(self):
        return None

    def __init__(self):
        collections.defaultdict.__init__(self, self.return_none)
# }}}
# {{{ Task Class


@dataclass(frozen=False)
class Task:
    # {{{ Instance variables
    WORKING_DIRECTORY: str
    TASK_STATE_FILEPATH: str = field(init=False)
    TASK_STATE_FILE_NAME: str = 'task_state.pkl'
    # task_state_dictionary: Dict[str, bool] = field(init=False)
    # task_dictionary: Dict[str, callable] = field(init=False)
    #     task_state_dictionary = collections.defaultdict(return_none)
    task_state_dictionary: Dict[str, bool] = field(
        default_factory=custom_default_dict)
    task_dictionary: Dict[str, callable] = field(default_factory=dict)
    task_dag: Dict[str, str] = field(default_factory=dict, init=False)
    # }}}
    # {{{ Properties

    @property
    def valid_tasks(self):
        return self.task_dictionary.keys()
    # }}}
    # {{{  return_none function
    # def return_none():
    #     return None
    # }}}
# {{{ post init

    def __post_init__(self):
        object.__setattr__(
            self,
            "TASK_STATE_FILEPATH",
            os.path.join(
                self.WORKING_DIRECTORY,
                self.TASK_STATE_FILE_NAME))
        # }}}
        # {{{ Clean directory

    @MyTracePath.inspect_function_execution
    def clean_directory(self):
        """Task function: cleans the working directory."""
        if os.path.exists(
                self.WORKING_DIRECTORY) and os.path.isdir(
                self.WORKING_DIRECTORY):
            shutil.rmtree(self.WORKING_DIRECTORY)
        assert(not os.path.exists(self.WORKING_DIRECTORY))
        print(f"Directory {self.WORKING_DIRECTORY} no longer exists.")
        for key in self.task_state_dictionary.keys():
            self.task_state_dictionary[key] = None
    # }}}
    # {{{ pipeline function

    @MyTracePath.inspect_function_execution
    def pipeline_function(self, prefix_string, preprocessing_fn, mydataclass):
        """
        Use a beam pipeline to transform data using preprocessing_fn. Write out
        data as tfrecords, and also transform_fn_output so pipeline can be reused.
        Reads from raw data and uses schema to interpret the data before using
        preprocessing_fn to transform it.
        """
        # with beam.Pipeline(options=pipeline_options) as pipeline:
        with beam.Pipeline() as pipeline:
            with tft_beam.Context(temp_dir=tempfile.mkdtemp()):
                # Create a TFXIO to read the data with the schema. To do this we
                # need to list all columns in order since the schema doesn't specify
                # the order of columns in the csv.
                # We first read CSV files and use BeamRecordCsvTFXIO whose
                # .BeamSource() accepts a PCollection[bytes]
                # tfxio.CsvTFXIO can be used
                # to both read the CSV files and parse them to TFT inputs:
                # csv_tfxio = tfxio.CsvTFXIO(...)
                # raw_data = (pipeline |
                # 'ToRecordBatches' >> csv_tfxio.BeamSource())
                csv_tfxio = tfxio.BeamRecordCsvTFXIO(
                    physical_format='text',
                    column_names=mydataclass.CSV_COLUMNS,
                    schema=mydataclass._SCHEMA)

                # Read in raw data and convert using CSV TFXIO.  Note that we apply
                # some Beam transformations here, which will not be encoded in the
                # TF graph since we don't do the from within tf.Transform's methods
                # (AnalyzeDataset, TransformDataset etc.).  These transformations
                #  are just to get data into a format that the CSV TFXIO can read,
                #  in particular removing spaces after commas.
                raw_data = (
                    pipeline | 'ReadTrainData' >> beam.io.ReadFromText(
                        file_pattern=mydataclass.TRAIN_FILE_PATH,
                        coder=beam.coders.BytesCoder(),
                        skip_header_lines=1) | 'DecodeTrainData' >> csv_tfxio.BeamSource())
                raw_dataset = (raw_data, csv_tfxio.TensorAdapterConfig())

                transformed_dataset, transform_fn = (
                    raw_dataset | tft_beam.AnalyzeAndTransformDataset(
                        preprocessing_fn, output_record_batches=True))

            # Transformed metadata is not necessary for encoding.
                transformed_data, _ = transformed_dataset

            # Extract transformed RecordBatches, encode and write them to the given
            # directory.
                tfrecord_directory = os.path.join(
                    self.WORKING_DIRECTORY, prefix_string)
                if os.path.exists(tfrecord_directory) and os.path.isdir(
                        tfrecord_directory):
                    shutil.rmtree(tfrecord_directory)
                transform_fn_output = os.path.join(tfrecord_directory,
                                                   'transform_output')
                tfrecord_file_path_prefix = os.path.join(tfrecord_directory,
                                                         prefix_string)
                data_written = (transformed_data | 'EncodeTrainData' >> beam.FlatMapTuple(
                    lambda batch, x: RecordBatchToExamples(batch)) |
                    # lambda x, y: y) |
                    #    "Logging info" >> beam.Map(_logging) )
                    'WriteTrainData' >> beam.io.WriteToTFRecord(
                    tfrecord_file_path_prefix, ))
                _ = (
                    transform_fn
                    | "WriteTransformFn" >>
                    tft_beam.WriteTransformFn(transform_fn_output))
                return True if data_written else False
    # }}}
    # {{{ Write tfrecords

    @MyTracePath.inspect_function_execution
    def write_raw_tfrecords(self, mydataclass, preprocessing_fn):
        if self.task_state_dictionary["write_raw_tfrecords"] is None:
            original_prefix_string = 'raw_tfrecords'

            # def no_preprocessing_fn(x):
            #     return x.copy()

            self.pipeline_function(prefix_string=original_prefix_string,
                                   preprocessing_fn=preprocessing_fn,
                                   mydataclass=mydataclass)
            self.task_state_dictionary["write_raw_tfrecords"] = True
            # }}}
# {{{ Transform tfrecords

    @MyTracePath.inspect_function_execution
    def transform_tfrecords(self, mydataclass, preprocessing_fn):
        """
        Task function: Call the pipeline function with transform_tfrecords
        prefix and provide the preprocessing_fn, which creates transformed
        tfrecords.
        """
        prefix_string = mydataclass.PREFIX_STRING
        self.pipeline_function(
            mydataclass=mydataclass,
            prefix_string=prefix_string,
            preprocessing_fn=preprocessing_fn)
        self.task_state_dictionary["transform_tfrecords"] = True
        # }}}
        # {{{ check prerequisites

    @MyTracePath.inspect_function_execution
    def check_prerequisites(self, task_prerequisites):
        """Return whether or not all prequisites have been met for a task."""
        prerequisites_met = True
        for task in task_prerequisites:
            prerequisites_met = prerequisites_met and self.task_state_dictionary[task]
        return prerequisites_met
    # }}}
# {{{ inspect_example

    @MyTracePath.inspect_function_execution
    def inspect_example(self, dictionary_of_tensors):
        """Produce a description of dictionary of tensors"""
        list_of_keys = dictionary_of_tensors.keys()
        list_of_tensors = dictionary_of_tensors.values()
        list_of_shapes = [tensor.shape for tensor in list_of_tensors]
        list_of_dtypes = [tensor.dtype for tensor in list_of_tensors]
        for tensor_name, tensor_shape, tensor_dtype in zip(list_of_keys,
                                                           list_of_shapes,
                                                           list_of_dtypes):
            print(
                f"Tensor {tensor_name} has dtype {tensor_dtype} and"
                f"shape {tensor_shape}")
        return None
    # }}}
# {{{ get single batched example

    @MyTracePath.inspect_function_execution
    def get_single_batched_example(self, prefix_string):
        """
        Get a single example by reading from tfrecords and interpreting
        using the feature spec stored by tensorflow transform.
        """
        list_of_tfrecord_files = glob.glob(
            os.path.join(
                self.WORKING_DIRECTORY,
                prefix_string,
                prefix_string + '*'))
        dataset = tf.data.TFRecordDataset(list_of_tfrecord_files)
        tft_transform_output = self.get_tft_transform_output(
            prefix_string)
        feature_spec = tft_transform_output.transformed_feature_spec()
        # tft_layer = tft_transform_output.transform_features_layer()
        example_string = dataset.take(1).get_single_element()
        single_example = tf.io.parse_single_example(
            serialized=example_string, features=feature_spec)
        single_example_batched = {key: tf.reshape(
            single_example[key], (1, -1)) for key in single_example.keys()}
        return single_example_batched
    # }}}
        # {{{ View original sample data

    @MyTracePath.inspect_function_execution
    def view_original_sample_data(self):
        """
        Task function: Gets raw input data and shows the data along
        with datatypes and shapes.
        """
        task_prerequisites = self.task_dag['view_original_sample_data']
        if not self.check_prerequisites(task_prerequisites):
            # Do pre-reqs
            for task in task_prerequisites:
                if not self.task_state_dictionary[task]:
                    self.perform_task(task)
        assert(self.check_prerequisites(task_prerequisites))
        single_original_batch_example = self.get_single_batched_example(
            prefix_string='raw_tfrecords')
        print('-' * 80)
        print(single_original_batch_example)
        print('-' * 80)
        self.inspect_example(single_original_batch_example)
        # }}}
        # {{{ build raw inputs

    @MyTracePath.inspect_function_execution
    def build_raw_inputs(self, mydataclass):
        """Produce keras input layer for raw data for end-to-end model"""
        raw_inputs_for_training = {}
        for key, spec in mydataclass.RAW_DATA_FEATURE_SPEC.items():
            if isinstance(spec, tf.io.VarLenFeature):
                raw_inputs_for_training[key] = tf.keras.layers.Input(
                    shape=(1,), name=key, dtype=spec.dtype, sparse=True)
            elif isinstance(spec, tf.io.FixedLenFeature):
                raw_inputs_for_training[key] = tf.keras.layers.Input(
                    shape=spec.shape, name=key, dtype=spec.dtype)
            else:
                raise ValueError('Spec type is not supported: ', key, spec)
        return raw_inputs_for_training
    # }}}
    # {{{ get_tft_transform_output

    @MyTracePath.inspect_function_execution
    def get_tft_transform_output(self, prefix_string):
        """Get tft transform output from a specified location"""
        # below statement needed because of issue with graph loading
        tfa.text.parse_time(
            "2001-01-01 07:12:13 UTC",
            "%Y-%m-%d %H:%M:%S %Z",
            output_unit="SECOND")

        transform_fn_output_directory = os.path.join(
            self.WORKING_DIRECTORY, prefix_string, 'transform_output')
        # ipdb()
        tft_transform_output = tft.TFTransformOutput(
            transform_fn_output_directory)
        return tft_transform_output
    # }}}
    # {{{ build raw to preprocessing model

    @MyTracePath.inspect_function_execution
    def build_raw_to_preprocessing_model(
            self, raw_inputs, tft_transform_output):
        """
        Produce a model that transforms raw inputs into the non-embedding
        component of the model. This uses the tft_layer provided by tft.

        """
        tft_layer = tft_transform_output.transform_features_layer()
        return tf.keras.Model(raw_inputs, tft_layer(raw_inputs))

        # }}}
        # {{{ View transformed sample data

    @MyTracePath.inspect_function_execution
    def view_transformed_sample_data(self, mydataclass):
        """
        Take raw input, run it through preprocessing function, and show
        what it started out as, and what it got transformed to.
        """
        task_prerequisites = self.task_dag['view_transformed_sample_data']
        if not self.check_prerequisites(task_prerequisites):
            # Do pre-reqs
            for task in task_prerequisites:
                if not self.task_state_dictionary[task]:
                    self.perform_task(task)
        assert(self.check_prerequisites(task_prerequisites))
        prefix_string = mydataclass.PREFIX_STRING
        raw_inputs = self.build_raw_inputs(mydataclass)
        single_original_batch_example = self.get_single_batched_example(
            prefix_string='raw_tfrecords')
        tft_transform_output = self.get_tft_transform_output(
            prefix_string)
        raw_to_preprocessing_model = self.build_raw_to_preprocessing_model(
            raw_inputs, tft_transform_output)
        transformed_batch_example = raw_to_preprocessing_model(
            single_original_batch_example)
        print('-' * 80)
        print(f"Start out with: {single_original_batch_example}")
        print('-' * 80)
        print(f"End up with: {transformed_batch_example}")
        self.inspect_example(transformed_batch_example)
        # {{{

    @MyTracePath.inspect_function_execution
    def build_transformed_inputs(self, TRANSFORMED_DATA_FEATURE_SPEC):
        """
        Given a feature spec, produce a dictionary of keras input
        layers specifying shape, name, and datatype.
        """
        transformed_inputs = {}
        for key, spec in TRANSFORMED_DATA_FEATURE_SPEC.items():
            if isinstance(spec, tf.io.VarLenFeature):
                transformed_inputs[key] = tf.keras.layers.Input(
                    shape=(1,), name=key, dtype=spec.dtype, sparse=True)
            elif isinstance(spec, tf.io.FixedLenFeature):
                transformed_inputs[key] = tf.keras.layers.Input(
                    shape=spec.shape, name=key, dtype=spec.dtype)
            else:
                raise ValueError('Spec type is not supported: ', key, spec)
        return transformed_inputs

        # }}}
        # {{{
    @MyTracePath.inspect_function_execution
    def build_dnn_and_keras_inputs(self, transformed_inputs):
        """
        Splits transformed inputs into two: one part goes into the dense
        layer, the other part goes to keras preprocessing layers.
        """
        dnn_input_names = [
            'hashed_trip_and_time',
            'day_of_week',
            'euclidean',
            'pickup_longitude',
            'dropoff_location',
            'dropoff_longitude',
            'pickup_location',
            'passenger_count',
            'dropoff_latitude',
            'hour_of_day',
            'pickup_latitude',
            'hashed_trip',
            'hour_of_day_of_week',
            ]

        keras_preprocessing_input_names = [
            'bucketed_dropoff_longitude',
            'bucketed_dropoff_latitude',
            'bucketed_pickup_latitude',
            'bucketed_pickup_longitude',
            ]
        dnn_inputs = {}
        dnn_inputs = {
            input_name: transformed_inputs[input_name]
            for input_name in dnn_input_names}
        keras_preprocessing_inputs = {}
        keras_preprocessing_inputs = {
            input_name: transformed_inputs[input_name]
            for input_name in keras_preprocessing_input_names}
        return dnn_inputs, keras_preprocessing_inputs

        # }}}
        # {{{ build non embedding model

    @MyTracePath.inspect_function_execution
    def build_non_embedding_model(self, dnn_inputs, ):
        """
        Create a model without embeddings, attach it to its inputs,
        compile and return it.
        """
        # dnn_inputs, _ = build_dnn_and_keras_inputs(transformed_inputs)
        stacked_inputs = tf.concat(tf.nest.flatten(dnn_inputs), axis=1)
        ht1 = layers.Dense(32, activation='relu', name='ht1')(stacked_inputs)
        ht2 = layers.Dense(8, activation='relu', name='ht2')(ht1)
        output = layers.Dense(1, activation='linear', name='fare')(ht2)
        non_embedding_model = tf.keras.Model(dnn_inputs, output)
        non_embedding_model.compile(
            optimizer='adam', loss='mse', metrics=['mse'])
        return non_embedding_model

        # }}}
        # {{{ map features and labels
    @MyTracePath.inspect_function_execution
    def map_features_and_labels(self, mydataclass, example):
        """Split the label from the features"""
        label = example.pop(mydataclass.LABEL_COLUMN)
        return example, label
        # }}}
        # {{{ get transformed dataset

    @MyTracePath.inspect_function_execution
    def get_transformed_dataset(self, mydataclass, prefix_string):
        """
        Read the tfrecords on disk, get the feature spec stored to disk,
        split the dataset into X, y, batch and make infinite and
        return for training.
        """
        list_of_transformed_files = glob.glob(
            os.path.join(
                self.WORKING_DIRECTORY,
                prefix_string,
                prefix_string) + '*')
        transformed_ds = tf.data.TFRecordDataset(
            filenames=list_of_transformed_files)
        tft_transform_output = self.get_tft_transform_output(
            prefix_string=prefix_string)

        feature_spec = tft_transform_output.transformed_feature_spec().copy()
        transformed_ds = transformed_ds.map(
            lambda X: tf.io.parse_single_example(
                serialized=X, features=feature_spec))
        # transformed_ds = transformed_ds.map(lambda X: (X, X.pop("fare_amount")))
        transformed_ds = transformed_ds.map(
            lambda X: self.map_features_and_labels(
                mydataclass, X))

        transformed_ds = transformed_ds.batch(
            batch_size=mydataclass.BATCH_SIZE).repeat()
        return transformed_ds

        # }}}
        # {{{ train non embedding model

    @MyTracePath.inspect_function_execution
    def train_non_embedding_model(self, mydataclass):
        """
        Task function: trains a model without embeddings by:

        1. Ensuring pre-requisites are completed
        2. Creating an input layer for the model and splitting out non-embedding
           section.
        3. Creates a transformed dataset and trains the model on it.
        """
        task_prerequisites = self.task_dag['train_non_embedding_model']
        if not self.check_prerequisites(task_prerequisites):
            # Do pre-reqs
            for task in task_prerequisites:
                if not self.task_state_dictionary[task]:
                    self.perform_task(task)
        assert(self.check_prerequisites(task_prerequisites))
        # build_raw_inputs(RAW_DATA_FEATURE_SPEC)
        prefix_string = mydataclass.PREFIX_STRING
        tft_transform_output = self.get_tft_transform_output(
            prefix_string)
        TRANSFORMED_DATA_FEATURE_SPEC = tft_transform_output.transformed_feature_spec()
        TRANSFORMED_DATA_FEATURE_SPEC.pop(mydataclass.LABEL_COLUMN)
        transformed_inputs = self.build_transformed_inputs(
            TRANSFORMED_DATA_FEATURE_SPEC)
        dnn_inputs, keras_preprocessing_inputs = self.build_dnn_and_keras_inputs(
            transformed_inputs)
        non_embedding_model = self.build_non_embedding_model(
            dnn_inputs, )
        transformed_ds = self.get_transformed_dataset(
            mydataclass, prefix_string)
        non_embedding_model.fit(transformed_ds, epochs=8, steps_per_epoch=64)
        self.task_state_dictionary['train_non_embedding_model'] = True
        # }}}
        # {{{ Build embedding model

    @MyTracePath.inspect_function_execution
    def build_embedding_model(self, mydataclass, transformed_inputs):
        """
        Build, compile, and return a model that uses keras preprocessing
        layers in conjunction with the preprocessing already done in tensorflow
        transform.
        """
        dnn_inputs, keras_preprocessing_inputs = self.build_dnn_and_keras_inputs(
            transformed_inputs)
        bucketed_pickup_longitude_intermediary = keras_preprocessing_inputs[
            'bucketed_pickup_longitude']
        bucketed_pickup_latitude_intermediary = keras_preprocessing_inputs[
            'bucketed_pickup_latitude']
        bucketed_dropoff_longitude_intermediary = keras_preprocessing_inputs[
            'bucketed_dropoff_longitude']
        bucketed_dropoff_latitude_intermediary = keras_preprocessing_inputs[
            'bucketed_dropoff_latitude']
        hash_pickup_crossing_layer_intermediary = tf.keras.layers.experimental.preprocessing.HashedCrossing(
            output_mode='int', num_bins=mydataclass.NBUCKETS**2, )
        hashed_pickup_intermediary = hash_pickup_crossing_layer_intermediary(
            (bucketed_pickup_longitude_intermediary, bucketed_pickup_latitude_intermediary))
        hash_dropoff_crossing_layer_intermediary = tf.keras.layers.experimental.preprocessing.HashedCrossing(
            output_mode='int', num_bins=mydataclass.NBUCKETS**2, )
        hashed_dropoff_intermediary = hash_dropoff_crossing_layer_intermediary(
            (bucketed_dropoff_longitude_intermediary, bucketed_dropoff_latitude_intermediary))
        hash_trip_crossing_layer = tf.keras.layers.experimental.preprocessing.HashedCrossing(
            output_mode='one_hot', num_bins=mydataclass.NBUCKETS ** 3, name="hash_trip_crossing_layer")
        hashed_trip = hash_trip_crossing_layer(
            (hashed_pickup_intermediary,
             hashed_dropoff_intermediary))
        hashed_trip_and_time = dnn_inputs["hashed_trip_and_time"]

        stacked_inputs = tf.concat(tf.nest.flatten(dnn_inputs), axis=1)

        nht1 = layers.Dense(32, activation='relu', name='ht1')(stacked_inputs)
        nht2 = layers.Dense(8, activation='relu', name='ht2')(nht1)
        kp1 = layers.Dense(8, activation='relu',
                           name='kp1')(hashed_trip)

        # transformed_inputs = {}
        # transformed_inputs = tft_layer(raw_inputs_for_training)
        # transformed_inputs.pop(mydataclass.LABEL_COLUMN)
        # transformed_input = tft_layer(raw_inputs)
        trip_locations_embedding_layer = tf.keras.layers.Embedding(
            input_dim=mydataclass.NBUCKETS ** 3,
            output_dim=int(mydataclass.NBUCKETS ** 1.5),
            name="trip_locations_embedding_layer")
        trip_embedding_layer = tf.keras.layers.Embedding(
            input_dim=(mydataclass.NBUCKETS ** 3) * 4,
            output_dim=int(mydataclass.NBUCKETS ** 1.5),
            name="trip_embedding_layer")
        trip_locations_embedding = trip_locations_embedding_layer(hashed_trip)
        trip_embedding = trip_embedding_layer(hashed_trip_and_time)
        et1 = layers.LSTM(32, activation='tanh', name='et1')(
            trip_locations_embedding)
        et2 = layers.LSTM(32, activation='tanh', name='et2')(trip_embedding)
        merge_layer = layers.concatenate([nht2, kp1, et1, et2])
        nht3 = layers.Dense(16)(merge_layer)

        # final output is a linear activation because this is regression
        output = layers.Dense(1, activation='linear', name='fare')(nht3)
        new_model = tf.keras.Model(transformed_inputs, output)
        new_model.compile(optimizer='adam', loss='mse', metrics=['mse'])
        return new_model

        # }}}
        # {{{ Train embedding model

    @MyTracePath.inspect_function_execution
    def train_embedding_model(self, mydataclass):
        """
        Task function: trains a model with embeddings by:

        1. Ensuring pre-requisites are completed
        2. Creating an input layer for the model for both dense and preprocessing
           layers.
        3. Creates a transformed dataset and trains the model on it.
        """
        task_prerequisites = self.task_dag['train_embedding_model']
        if not self.check_prerequisites(task_prerequisites):
            # Do pre-reqs
            for task in task_prerequisites:
                if not self.task_state_dictionary[task]:
                    self.perform_task(task)
        assert(self.check_prerequisites(task_prerequisites))
        self.build_raw_inputs(mydataclass)
        prefix_string = mydataclass.PREFIX_STRING
        tft_transform_output = self.get_tft_transform_output(
            prefix_string)
        TRANSFORMED_DATA_FEATURE_SPEC = tft_transform_output.transformed_feature_spec()
        TRANSFORMED_DATA_FEATURE_SPEC.pop(mydataclass.LABEL_COLUMN)
        transformed_inputs = self.build_transformed_inputs(
            TRANSFORMED_DATA_FEATURE_SPEC)
        dnn_inputs, keras_preprocessing_inputs = self.build_dnn_and_keras_inputs(
            transformed_inputs)
        embedding_model = self.build_embedding_model(
            mydataclass, transformed_inputs)
        transformed_ds = self.get_transformed_dataset(
            mydataclass, prefix_string)
        embedding_model.fit(transformed_ds, epochs=1, steps_per_epoch=64)
        self.task_state_dictionary['train_embedding_model'] = True
        # }}}
        # {{{ end to end model

    @MyTracePath.inspect_function_execution
    def build_end_to_end_model(self,
                               raw_inputs,
                               transformed_inputs,
                               prefix_string,
                               new_model):
        """
        Connect the raw inputs to the trained model by using the tft_layer
        to transform raw data to a format that the model preprocessing pipeline
        already knows how to handle.
        """
        tft_transform_output = self.get_tft_transform_output(
            prefix_string)
        tft_layer = tft_transform_output.transform_features_layer()
        x = tft_layer(raw_inputs)
        outputs = new_model(x)
        end_to_end_model = tf.keras.Model(raw_inputs, outputs)
        return end_to_end_model

    # }}}
# {{{ Train and predict embedding model

    @MyTracePath.inspect_function_execution
    def train_and_predict_embedding_model(self, mydataclass):
        """
        Task function: trains a model with embeddings by:

        1. Ensuring pre-requisites are completed
        2. Creating an input layer for the model for both dense and preprocessing
           layers.
        3. Creates a transformed dataset and trains the model on it.
        4. Attaches the raw inputs to make an end-to-end graph that includes
           everything
        5. Takes a single raw input sample and does a prediction on it
        """
        task_prerequisites = self.task_dag['train_and_predict_embedding_model']
        if not self.check_prerequisites(task_prerequisites):
            # Do pre-reqs
            for task in task_prerequisites:
                if not self.task_state_dictionary[task]:
                    self.perform_task(task)
        assert(self.check_prerequisites(task_prerequisites))
        raw_inputs = self.build_raw_inputs(mydataclass)
        prefix_string = mydataclass.PREFIX_STRING
        single_original_batch_example = self.get_single_batched_example(
            prefix_string='raw_tfrecords')
        tft_transform_output = self.get_tft_transform_output(
            prefix_string)
        TRANSFORMED_DATA_FEATURE_SPEC = tft_transform_output.transformed_feature_spec()
        TRANSFORMED_DATA_FEATURE_SPEC.pop(mydataclass.LABEL_COLUMN)
        transformed_inputs = self.build_transformed_inputs(
            TRANSFORMED_DATA_FEATURE_SPEC)
        # dnn_inputs, keras_preprocessing_inputs = build_dnn_and_keras_inputs(
        #     transformed_inputs)
        embedding_model = self.build_embedding_model(
            mydataclass, transformed_inputs)
        transformed_ds = self.get_transformed_dataset(
            mydataclass, prefix_string)
        embedding_model.fit(transformed_ds, epochs=1, steps_per_epoch=64)
        end_to_end_model = self.build_end_to_end_model(
            raw_inputs,
            transformed_inputs,
            prefix_string,
            embedding_model)
        print(end_to_end_model.predict(single_original_batch_example))
        self.task_state_dictionary['train_and_predict_embedding_model'] = True
# }}}

    # }}}
# {{{ Perform task
    @MyTracePath.inspect_function_execution
    def perform_task(self, task_name):
        """
        Execute task functions stored in the task_dictionary.
        """
        if task_name in self.valid_tasks:
            self.task_dictionary[task_name]()

    @MyTracePath.inspect_function_execution
    def closeout_task(self):
        """Persist the task state information to disk"""
        if os.path.exists(
                self.WORKING_DIRECTORY) and os.path.isdir(
                self.WORKING_DIRECTORY):
            with open(self.TASK_STATE_FILEPATH, 'wb') as task_state_file:
                pickle.dump(
                    dict(self.task_state_dictionary),
                    task_state_file,
                    protocol=pickle.HIGHEST_PROTOCOL)


# }}}
# }}}
# {{{ Time since 1970 function
# can't be used because of likely bug in tensorflow, tried to make a class
# method, does not work.  Only works if at the module level, hence decided to
# skip the function and execute operations directly where needed.
# @tf.function
# def fn_seconds_since_1970(ts_in):
#     """Compute the seconds since 1970 given a datetime string"""
#     seconds_since_1970 = tfa.text.parse_time(
#         ts_in, "%Y-%m-%d %H:%M:%S %Z", output_unit="SECOND"
#         )
#     seconds_since_1970 = tf.cast(seconds_since_1970, tf.float32)
#     return seconds_since_1970
# }}}
# {{{ Parser function
# @MyTracePath.inspect_function_execution
def get_args():
    valid_tasks = [
        'clean_directory',
        'write_raw_tfrecords',
        'transform_tfrecords',
        'view_original_sample_data',
        'view_transformed_sample_data',
        'train_non_embedding_model',
        'train_embedding_model',
        'train_and_predict_embedding_model']
    parser = argparse.ArgumentParser(
        prog="tft_tasks.py",
        description="A task based approach to using tensorflow transform.")
    parser.add_argument(
        '--task',
        dest='tasks',
        action='append',
        required=True,
        # help=f'Pick tasks from {valid_tasks}')
        help=f'Pick tasks from {valid_tasks}')
    parser.add_argument(
        '--visualize_tasks',
        dest='visualization_filename',
        action='append',
        required=False,
        help='Specify the filename to visualize the execution of tft tasks'
        '(e.g. mlops_pipeline.svg)')
    parser.add_argument(
        '--print_performance_metrics',
        dest='print_performance',
        action='store_true',
        required=False,
        help='specify if you want performance metrics to be printed to the\
        console')
    parser.set_defaults(print_performance=False)
    return parser.parse_args()
# }}}
# {{{ Main function


def main(args):
    # {{{ Constants
    """
    Manages the task state information and calls the various tasks given
    input from argparse
    """
    CSV_COLUMNS = [
        "key",
        "fare_amount",
        "pickup_datetime",
        "pickup_longitude",
        "pickup_latitude",
        "dropoff_longitude",
        "dropoff_latitude",
        "passenger_count",
        ]

    LABEL_COLUMN = "fare_amount"

    STRING_COLS = ["pickup_datetime"]

    NUMERIC_COLS = [
        "pickup_longitude",
        "pickup_latitude",
        "dropoff_longitude",
        "dropoff_latitude",
        "passenger_count",
        ]

    DEFAULTS = [["na"], [0.0], ["na"], [0.0], [0.0], [0.0], [0.0], [0.0], ]

    DEFAULT_DTYPES = [
        tf.float32,
        tf.string,
        tf.float32,
        tf.float32,
        tf.float32,
        tf.float32,
        tf.float32]

    ALL_DTYPES = [
        tf.string,
        tf.float32,
        tf.string,
        tf.float32,
        tf.float32,
        tf.float32,
        tf.float32,
        tf.float32]

    NBUCKETS = 10

    TEST_FILE_PATH = './data/taxi-valid.csv'
    TRAIN_FILE_PATH = './data/taxi-train_toy.csv'
    WORKING_DIRECTORY = './working_area'
    BATCH_SIZE = 8
    PREFIX_STRING = 'transformed_tfrecords'
    # }}}
# {{{ Instantiating instances
    my_taxicab_data = TaxiCabData(
        TEST_FILE_PATH=TEST_FILE_PATH,
        TRAIN_FILE_PATH=TRAIN_FILE_PATH,
        NBUCKETS=NBUCKETS,
        CSV_COLUMNS=CSV_COLUMNS,
        LABEL_COLUMN=LABEL_COLUMN,
        STRING_COLS=STRING_COLS,
        NUMERIC_COLS=NUMERIC_COLS,
        DEFAULTS=DEFAULTS,
        DEFAULT_DTYPES=DEFAULT_DTYPES,
        ALL_DTYPES=ALL_DTYPES,
        BATCH_SIZE=BATCH_SIZE,
        PREFIX_STRING=PREFIX_STRING)
    my_tasks = Task(
        WORKING_DIRECTORY=WORKING_DIRECTORY,
        # task_state_dictionary = {},
        # task_dictionary = {},
        # task_dag = task_dag
        )
# }}}
# {{{ cleaning up variables
    del(TEST_FILE_PATH)
    del(TRAIN_FILE_PATH)
    del(NBUCKETS)
    del(CSV_COLUMNS)
    del(LABEL_COLUMN)
    del(STRING_COLS)
    del(NUMERIC_COLS)
    del(DEFAULTS)
    del(DEFAULT_DTYPES)
    del(ALL_DTYPES)
    del(BATCH_SIZE)
    del(PREFIX_STRING)
    del(WORKING_DIRECTORY)
    # }}}
# {{{ no preprocessing function

    def no_preprocessing_fn(inputs):
        """Preprocess input columns into transformed columns."""
        # Since we are modifying some features and leaving others unchanged, we
        # start by setting `outputs` to a copy of `inputs.
        outputs = inputs.copy()
        return outputs
    # }}}
# {{{ preprocessing function

    def preprocessing_fn(inputs, mydataclass):
        """
        Preprocess input columns into transformed features. This is what goes
        into tensorflow transform/apache beam.
        """
        # Since we are modifying some features and leaving others unchanged, we
        # start by setting `outputs` to a copy of `inputs.
        transformed = inputs.copy()
        transformed['passenger_count'] = tft.scale_to_0_1(
            inputs['passenger_count'])
        # cannot use the below in tft as managing those learned values needs to be
        # managed carefully
        # normalizer = tf.keras.layers.Normalization(axis=None,
        # name="passenger_count_normalizer")
        # normalizer.adapt(inputs['passenger_count'])
        # transformed['other_passenger_count'] = normalizer(
        #               inputs['passenger_count'])
        for lon_col in ['pickup_longitude', 'dropoff_longitude']:
            # transformed[lon_col] = scale_longitude(inputs[lon_col])
            transformed[lon_col] = (inputs[lon_col] + 78) / 8.
        for lat_col in ['pickup_latitude', 'dropoff_latitude']:
            transformed[lat_col] = (inputs[lat_col] - 37) / 8.
        position_difference = tf.square(
            inputs["dropoff_longitude"] -
            inputs["pickup_longitude"])
        position_difference += tf.square(
            inputs["dropoff_latitude"] -
            inputs["pickup_latitude"])
        transformed['euclidean'] = tf.sqrt(position_difference)
        lat_lon_buckets = [
            bin_edge / mydataclass.NBUCKETS
            for bin_edge in range(0, mydataclass.NBUCKETS)]

        transformed['bucketed_pickup_longitude'] = tft.apply_buckets(
            transformed["pickup_longitude"],
            bucket_boundaries=tf.constant([lat_lon_buckets]))
        transformed["bucketed_pickup_latitude"] = tft.apply_buckets(
            transformed['pickup_latitude'],
            bucket_boundaries=tf.constant([lat_lon_buckets]))

        transformed['bucketed_dropoff_longitude'] = tft.apply_buckets(
            transformed["dropoff_longitude"],
            bucket_boundaries=tf.constant([lat_lon_buckets]))
        transformed['bucketed_dropoff_latitude'] = tft.apply_buckets(
            transformed["dropoff_latitude"],
            bucket_boundaries=tf.constant([lat_lon_buckets]))

        # transformed["pickup_cross"]=tf.sparse.cross(
        # inputs=[transformed['pickup_latitude_apply_buckets'],
        # transformed['pickup_longitude_apply_buckets']])
        hash_pickup_crossing_layer = tf.keras.layers.experimental.preprocessing.HashedCrossing(
            output_mode='one_hot', num_bins=mydataclass.NBUCKETS**2, name='hash_pickup_crossing_layer')
        transformed['pickup_location'] = hash_pickup_crossing_layer(
            (transformed['bucketed_pickup_latitude'],
             transformed['bucketed_pickup_longitude']))
        hash_dropoff_crossing_layer = tf.keras.layers.experimental.preprocessing.HashedCrossing(
            output_mode='one_hot', num_bins=mydataclass.NBUCKETS**2,
            name='hash_dropoff_crossing_layer')
        transformed['dropoff_location'] = hash_dropoff_crossing_layer(
            (transformed['bucketed_dropoff_latitude'],
             transformed['bucketed_dropoff_longitude']))

        hash_pickup_crossing_layer_intermediary = tf.keras.layers.experimental.preprocessing.HashedCrossing(
            output_mode='int', num_bins=mydataclass.NBUCKETS**2, )
        hashed_pickup_intermediary = hash_pickup_crossing_layer_intermediary(
            (transformed['bucketed_pickup_longitude'],
             transformed['bucketed_pickup_latitude']))
        hash_dropoff_crossing_layer_intermediary = tf.keras.layers.experimental.preprocessing.HashedCrossing(
            output_mode='int', num_bins=mydataclass.NBUCKETS**2, )
        hashed_dropoff_intermediary = hash_dropoff_crossing_layer_intermediary(
            (transformed['bucketed_dropoff_longitude'],
             transformed['bucketed_dropoff_latitude']))

        hash_trip_crossing_layer = tf.keras.layers.experimental.preprocessing.HashedCrossing(
            output_mode='one_hot', num_bins=mydataclass.NBUCKETS ** 3,
            name="hash_trip_crossing_layer")
        transformed['hashed_trip'] = hash_trip_crossing_layer(
            (hashed_pickup_intermediary,
             hashed_dropoff_intermediary))

        seconds_since_1970 = tf.cast(
            tfa.text.parse_time(
                inputs["pickup_datetime"],
                "%Y-%m-%d %H:%M:%S %Z",
                output_unit="SECOND"),
            tf.float32)

        # seconds_since_1970 = fn_seconds_since_1970(inputs['pickup_datetime'])
        seconds_since_1970 = tf.cast(seconds_since_1970, tf.float32)
        hours_since_1970 = seconds_since_1970 / 3600.
        hours_since_1970 = tf.floor(hours_since_1970)
        hour_of_day_intermediary = hours_since_1970 % 24
        transformed['hour_of_day'] = hour_of_day_intermediary
        hour_of_day_intermediary = tf.cast(hour_of_day_intermediary, tf.int32)
        days_since_1970 = seconds_since_1970 / (3600 * 24)
        days_since_1970 = tf.floor(days_since_1970)
        # January 1st 1970 was a Thursday
        day_of_week_intermediary = (days_since_1970 + 4) % 7
        transformed['day_of_week'] = day_of_week_intermediary
        day_of_week_intermediary = tf.cast(day_of_week_intermediary, tf.int32)
        hashed_crossing_layer = tf.keras.layers.experimental.preprocessing.HashedCrossing(
            num_bins=24 * 7, output_mode="one_hot")
        hashed_crossing_layer_intermediary = tf.keras.layers.experimental.preprocessing.HashedCrossing(
            num_bins=24 * 7, output_mode="int", name='hashed_hour_of_day_of_week_layer')
        transformed['hour_of_day_of_week'] = hashed_crossing_layer(
            (hour_of_day_intermediary, day_of_week_intermediary))
        hour_of_day_of_week_intermediary = hashed_crossing_layer_intermediary(
            (hour_of_day_intermediary, day_of_week_intermediary))

        hash_trip_crossing_layer_intermediary = tf.keras.layers.experimental.preprocessing.HashedCrossing(
            output_mode='int', num_bins=mydataclass.NBUCKETS ** 3)
        hashed_trip_intermediary = hash_trip_crossing_layer_intermediary(
            (hashed_pickup_intermediary, hashed_dropoff_intermediary))

        hash_trip_and_time_layer = tf.keras.layers.experimental.preprocessing.HashedCrossing(
            output_mode='one_hot', num_bins=(
                mydataclass.NBUCKETS ** 3) * 4, name='hash_trip_and_time_layer')
        transformed['hashed_trip_and_time'] = hash_trip_and_time_layer(
            (hashed_trip_intermediary, hour_of_day_of_week_intermediary))
        return transformed
    # }}}
    # {{{ Task setup
    my_tasks.task_dictionary["clean_directory"] = my_tasks.clean_directory
    my_tasks.task_dictionary["write_raw_tfrecords"] = partial(
        my_tasks.write_raw_tfrecords, mydataclass=my_taxicab_data,
        preprocessing_fn=no_preprocessing_fn)
    new_preprocessing_fn = partial(
        preprocessing_fn,
        mydataclass=my_taxicab_data)

    my_tasks.task_dictionary["transform_tfrecords"] = partial(
        my_tasks.transform_tfrecords,
        preprocessing_fn=new_preprocessing_fn,
        mydataclass=my_taxicab_data)
    my_tasks.task_dictionary["view_original_sample_data"] = my_tasks.view_original_sample_data
    my_tasks.task_dictionary["view_transformed_sample_data"] = partial(
        my_tasks.view_transformed_sample_data, mydataclass=my_taxicab_data)
    my_tasks.task_dictionary["train_non_embedding_model"] = partial(
        my_tasks.train_non_embedding_model, mydataclass=my_taxicab_data)
    my_tasks.task_dictionary["train_embedding_model"] = partial(
        my_tasks.train_embedding_model, mydataclass=my_taxicab_data)
    my_tasks.task_dictionary["train_and_predict_embedding_model"] = partial(
        my_tasks.train_and_predict_embedding_model, mydataclass=my_taxicab_data)

    my_tasks.task_dag = {key: [] for key in my_tasks.task_dictionary.keys()}
    # ipdb()
    my_tasks.task_dag['view_original_sample_data'].append(
        'write_raw_tfrecords')
    my_tasks.task_dag['view_transformed_sample_data'].append(
        'write_raw_tfrecords')
    my_tasks.task_dag['view_transformed_sample_data'].append(
        'transform_tfrecords')
    my_tasks.task_dag['train_non_embedding_model'].append(
        'transform_tfrecords')
    my_tasks.task_dag['train_embedding_model'].append('transform_tfrecords')
    my_tasks.task_dag['train_and_predict_embedding_model'].append(
        'write_raw_tfrecords')
    my_tasks.task_dag['train_and_predict_embedding_model'].append(
        'transform_tfrecords')
    # }}}
    # {{{ Task execution and closeout
    if os.path.isfile(my_tasks.TASK_STATE_FILEPATH):
        with open(my_tasks.TASK_STATE_FILEPATH, 'rb') as task_state_file:
            # my_taxicab_data.task_state_dictionary = pickle.load(
            #     task_state_file)
            place_holder_dict = pickle.load(task_state_file)
            my_tasks.task_state_dictionary.update(place_holder_dict)
    for task in args.tasks:
        if task not in my_tasks.valid_tasks:
            print(f"Invalid task {task}."
                  f"Pick tasks from {list(my_tasks.valid_tasks)}")

            continue
        else:
            my_tasks.perform_task(task)
    my_tasks.closeout_task()
    # }}}

    # }}}
# {{{ Allow importing into other programs


if __name__ == '__main__':
    """
    Gets input from argparse, calls main with those inputs, and
    produces the visualization if requested by the user
    """
    args = get_args()
    main(args)
    if args.visualization_filename:
        MyTracePath.construct_graph()
        MyTracePath.draw_graph(filename=args.visualization_filename[0])
    if args.print_performance:
        MyTracePath.display_performance()
# }}}
