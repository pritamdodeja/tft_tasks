#!/usr/bin/env python
# coding: utf-8
# {{{ Imports
# Goal: Implement taxicab fare prediction with a tft pipeline with safety
# checks and a simpler flow.
import tensorflow_metadata
from typing import List, Dict, FrozenSet, Set
from dataclasses import dataclass, field
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
from operator import itemgetter
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'  # or any {'0', '1', '2'}
# set TF error log verbosity
MyTracePath = TracePath(instrument=True, name="MyTracePath")
logger = logging.getLogger("tensorflow").setLevel(logging.INFO)
# print(tf.version.VERSION)
# }}}
# {{{ MLMetaData Data Class


@dataclass(frozen=True)
class MLMetaData:
    NBUCKETS: int = field(init=True)
    LABEL_COLUMN: str = field(init=True)
    TEST_FILE_PATH: str = field(init=True)
    TRAIN_FILE_PATH: str = field(init=True)
    TRANSFORMED_DATA_LOCATION: str = field(init=True)
    RAW_DATA_LOCATION: str = field(init=True)
    DEFAULT_DTYPES: List[tf.DType] = field(init=True)
    ALL_DTYPES: List[tf.DType] = field(init=True)
    CSV_COLUMNS: List[str] = field(default_factory=list)
    STRING_COLS: List[str] = field(default_factory=list)
    NUMERIC_COLS: List[str] = field(default_factory=list)
    DEFAULTS: List = field(default_factory=list)
    RAW_DATA_DICTIONARY: Dict[str, tf.DType] = field(init=False)
    RAW_DATA_FEATURE_SPEC: Dict[str, tf.DType] = field(init=False)
    _SCHEMA: tensorflow_metadata.proto.v0.schema_pb2.Schema = field(init=False)
    BATCH_SIZE: int = 8

    def __post_init__(self):
        raw_data_dictionary = {
            key: value for key, value in zip(
                self.CSV_COLUMNS, self.ALL_DTYPES
                )
            }
        object.__setattr__(self, "RAW_DATA_DICTIONARY", raw_data_dictionary)
        raw_data_feature_spec = {
            raw_feature_name: tf.io.FixedLenFeature(
                shape=(1,),
                dtype=self.RAW_DATA_DICTIONARY[raw_feature_name])
            for raw_feature_name in self.CSV_COLUMNS}
        object.__setattr__(
            self,
            "RAW_DATA_FEATURE_SPEC",
            raw_data_feature_spec)
        schema = schema_utils.schema_from_feature_spec(
            self.RAW_DATA_FEATURE_SPEC)
        object.__setattr__(self, "_SCHEMA", schema)

# }}}
# {{{ Custom default dict class


class CustomDefaultDict(collections.defaultdict):
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
    task_completed: Dict[str, bool] = field(
        default_factory=CustomDefaultDict)
    task_dictionary: Dict[str, callable] = field(default_factory=dict)
    task_dag: Dict[str, List[str]] = field(default_factory=dict, init=False)
    # }}}
    # {{{ Class variables
    valid_tasks: FrozenSet[Set[str]] = frozenset({
        'clean_directory',
        'preprocess_raw_data',
        'transform_raw_data',
        'view_original_sample_data',
        'view_transformed_sample_data',
        'train_non_embedding_model',
        'train_embedding_model',
        'train_and_predict_non_embedding_model',
        'train_and_predict_embedding_model'
        })
    # }}}
# {{{ post init def __post_init__(self):

    def __post_init__(self):
        object.__setattr__(
            self,
            "TASK_STATE_FILEPATH",
            os.path.join(
                self.WORKING_DIRECTORY,
                self.TASK_STATE_FILE_NAME))
        # }}}
        # {{{ check prerequisites def check_prerequisites(self, task_name):

    @MyTracePath.inspect_function_execution
    def check_prerequisites(self, task_name):
        """Return whether or not all prequisites have been met for a task."""
        prerequisites_met = True
        task_prerequisites = self.task_dag[task_name]
        for task_name in task_prerequisites:
            prerequisites_met = prerequisites_met and self.task_completed[task_name]
        return prerequisites_met
    # }}}
    # {{{ Perform prerequisites def perform_prerequisites(self, task):

    @MyTracePath.inspect_function_execution
    def perform_prerequisites(self, task_name):
        task_prerequisites = self.task_dag[task_name]
        for prerequisite_task_name in task_prerequisites:
            # Two criteria: prerequisites met and task hasn't already been done
            if self.check_prerequisites(prerequisite_task_name):
                if not self.task_completed[prerequisite_task_name]:
                    self.perform_task(prerequisite_task_name)
        assert(self.check_prerequisites(task_name))
    # }}}
        # {{{ Clean directory def clean_directory(self):

    @MyTracePath.inspect_function_execution
    def clean_directory(self):
        task_name = 'clean_directory'
        self.perform_prerequisites(task_name=task_name)
        """Task function: cleans the working directory."""
        if os.path.exists(
                self.WORKING_DIRECTORY) and os.path.isdir(
                self.WORKING_DIRECTORY):
            shutil.rmtree(self.WORKING_DIRECTORY)
        assert(not os.path.exists(self.WORKING_DIRECTORY))
        print(f"Directory {self.WORKING_DIRECTORY} no longer exists.")
        for key in self.task_completed.keys():
            self.task_completed[key] = None
    # }}}
    # {{{ get_tft_transform_output def get_tft_transform_output(self, location):

    @MyTracePath.inspect_function_execution
    def get_tft_transform_output(self, location):
        """Get tft transform output from a specified location"""
        # below statement needed because of issue with graph loading
        tfa.text.parse_time(
            "2001-01-01 07:12:13 UTC",
            "%Y-%m-%d %H:%M:%S %Z",
            output_unit="SECOND")

        transform_fn_output_directory = os.path.join(
            self.WORKING_DIRECTORY, location, 'transform_output')
        tft_transform_output = tft.TFTransformOutput(
            transform_fn_output_directory)
        return tft_transform_output
    # }}}
    # {{{ Get feature spec

    @MyTracePath.inspect_function_execution
    def get_feature_spec(self, location):
        tft_transform_output = self.get_tft_transform_output(location=location)
        feature_spec = tft_transform_output.transformed_feature_spec()
        return feature_spec
# }}}
# {{{ get single batched example def get_single_batched_example(self, location):

    @MyTracePath.inspect_function_execution
    def get_single_batched_example(self, location):
        """
        Get a single example by reading from tfrecords and interpreting
        using the feature spec stored by tensorflow transform.
        """
        list_of_tfrecord_files = glob.glob(
            os.path.join(
                self.WORKING_DIRECTORY,
                location,
                location + '*'))
        dataset = tf.data.TFRecordDataset(list_of_tfrecord_files)
        feature_spec = self.get_feature_spec(location=location)
        example_string = dataset.take(1).get_single_element()
        single_example = tf.io.parse_single_example(
            serialized=example_string, features=feature_spec)
        single_example_batched = {key: tf.reshape(
            single_example[key], (1, -1)) for key in single_example.keys()}
        return single_example_batched
    # }}}
# {{{ inspect_example def inspect_example(self, dictionary_of_tensors):

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
                f"Tensor {tensor_name} has dtype {tensor_dtype} and "
                f"shape {tensor_shape}")
        return None
    # }}}
        # {{{ def build_inputs(self, TRANSFORMED_DATA_FEATURE_SPEC):

    @MyTracePath.inspect_function_execution
    def build_inputs(self, FEATURE_SPEC):
        """
        Given a feature spec, produce a dictionary of keras input
        layers specifying shape, name, and datatype.
        """
        inputs = {}
        for key, spec in FEATURE_SPEC.items():
            if isinstance(spec, tf.io.VarLenFeature):
                inputs[key] = tf.keras.layers.Input(
                    shape=(1,), name=key, dtype=spec.dtype, sparse=True)
            elif isinstance(spec, tf.io.FixedLenFeature):
                inputs[key] = tf.keras.layers.Input(
                    shape=spec.shape, name=key, dtype=spec.dtype)
            else:
                raise ValueError('Spec type is not supported: ', key, spec)
        return inputs

        # }}}
    # {{{ pipeline function def tft_pipeline(self, 
    #              mydataclass, preprocessing_fn, location):

    @MyTracePath.inspect_function_execution
    def tft_pipeline(self, mydataclass, preprocessing_fn, location):
        """
        Use a beam pipeline to transform data using preprocessing_fn. Write out
        data as tfrecords, and also transform_fn_output so pipeline can be 
        reused. Reads from raw data and uses schema to interpret the data 
        before using preprocessing_fn to transform it.
        """
        # with beam.Pipeline(options=pipeline_options) as pipeline:
        with beam.Pipeline() as pipeline:
            with tft_beam.Context(temp_dir=tempfile.mkdtemp()):
                csv_tfxio = tfxio.BeamRecordCsvTFXIO(
                    physical_format='text',
                    column_names=mydataclass.CSV_COLUMNS,
                    schema=mydataclass._SCHEMA)
                raw_data = (
                    pipeline | 'ReadTrainData' >> beam.io.ReadFromText(
                        file_pattern=mydataclass.TRAIN_FILE_PATH,
                        coder=beam.coders.BytesCoder(),
                        skip_header_lines=1) | 
                            'DecodeTrainData' >> csv_tfxio.BeamSource())
                raw_dataset = (raw_data, csv_tfxio.TensorAdapterConfig())

                transformed_dataset, transform_fn = (
                    raw_dataset | tft_beam.AnalyzeAndTransformDataset(
                        preprocessing_fn, output_record_batches=True))
                transformed_data, _ = transformed_dataset
                tfrecord_directory = os.path.join(
                    self.WORKING_DIRECTORY, location)
                if os.path.exists(tfrecord_directory) and os.path.isdir(
                        tfrecord_directory):
                    shutil.rmtree(tfrecord_directory)
                transform_fn_output = os.path.join(tfrecord_directory,
                                                   'transform_output')
                tfrecord_file_path_prefix = os.path.join(tfrecord_directory,
                                                         location)
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
    # {{{ Write raw tfrecords def preprocess_raw_data(self, mydataclass, 
    # preprocessing_fn=lambda X:

    @MyTracePath.inspect_function_execution
    def preprocess_raw_data(self, mydataclass, preprocessing_fn=lambda X:
                            X.copy()):
        task_name = 'preprocess_raw_data'
        self.perform_prerequisites(task_name=task_name)
        self.tft_pipeline(mydataclass=mydataclass,
                          preprocessing_fn=preprocessing_fn,
                          location=mydataclass.RAW_DATA_LOCATION
                          )
        self.task_completed["preprocess_raw_data"] = True
        # }}}
# {{{ Transform tfrecords def transform_raw_data(self, mydataclass, 
    # preprocessing_fn):

    @MyTracePath.inspect_function_execution
    def transform_raw_data(self, mydataclass, preprocessing_fn):
        """
        Task function: Call the pipeline function with transform_raw_data
        prefix and provide the preprocessing_fn, which creates transformed
        tfrecords.
        """
        task_name = 'transform_raw_data'
        self.perform_prerequisites(task_name=task_name)
        # self.write_tfrecords(mydataclass=mydataclass,
        #                      preprocessing_fn=preprocessing_fn,
        #                      location=mydataclass.TRANSFORMED_DATA_LOCATION)
        self.tft_pipeline(mydataclass=mydataclass,
                          preprocessing_fn=preprocessing_fn,
                          location=mydataclass.TRANSFORMED_DATA_LOCATION
                          )
        self.task_completed["transform_raw_data"] = True
        # }}}
        # {{{ View original sample data def view_original_sample_data(self):

    @MyTracePath.inspect_function_execution
    def view_original_sample_data(self, mydataclass):
        """
        Task function: Gets raw input data and shows the data along
        with datatypes and shapes.
        """
        task_name = 'view_original_sample_data'
        self.perform_prerequisites(task_name=task_name)
        single_original_batch_example = self.get_single_batched_example(
            location=mydataclass.RAW_DATA_LOCATION)
        print('-' * 80)
        print(single_original_batch_example)
        print('-' * 80)
        self.inspect_example(single_original_batch_example)
        # }}}
    # {{{ build raw to preprocessing model def build_raw_to_preprocessing_model(

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
        # {{{ View transformed sample data def view_transformed_sample_
        # data(self, mydataclass):

    @MyTracePath.inspect_function_execution
    def view_transformed_sample_data(self, mydataclass):
        """
        Take raw input, run it through preprocessing function, and show
        what it started out as, and what it got transformed to.
        """
        task_name = 'view_transformed_sample_data'
        self.perform_prerequisites(task_name=task_name)
        location = mydataclass.TRANSFORMED_DATA_LOCATION
        raw_inputs = self.build_raw_inputs(mydataclass)
        single_original_batch_example = self.get_single_batched_example(
            location=mydataclass.RAW_DATA_LOCATION)
        tft_transform_output = self.get_tft_transform_output(
            location)
        raw_to_preprocessing_model = self.build_raw_to_preprocessing_model(
            raw_inputs, tft_transform_output)
        transformed_batch_example = raw_to_preprocessing_model(
            single_original_batch_example)
        print('-' * 80)
        print(f"Start out with: {single_original_batch_example}")
        print('-' * 80)
        print(f"End up with: {transformed_batch_example}")
        self.inspect_example(transformed_batch_example)
        # }}}
        # {{{ build raw inputs def build_raw_inputs(self, mydataclass):

    @MyTracePath.inspect_function_execution
    def build_raw_inputs(self, mydataclass):
        """Produce keras input layer for raw data for end-to-end model"""
        FEATURE_SPEC_COPY = mydataclass.RAW_DATA_FEATURE_SPEC.copy()
        FEATURE_SPEC_COPY.pop(mydataclass.LABEL_COLUMN)
        raw_inputs_for_training = self.build_inputs(
            FEATURE_SPEC=FEATURE_SPEC_COPY)
        return raw_inputs_for_training
    # }}}
        # {{{ build model inputs def build_model_inputs(self, mydataclass):

    @MyTracePath.inspect_function_execution
    def build_model_inputs(self, mydataclass):
        """
        Produce keras input layer for transformed data for end-to-end model
        """
        TRANSFORMED_DATA_FEATURE_SPEC = self.get_feature_spec(
            location=mydataclass.TRANSFORMED_DATA_LOCATION)
        TRANSFORMED_DATA_FEATURE_SPEC.pop(mydataclass.LABEL_COLUMN)
        model_inputs_for_training = self.build_inputs(
            FEATURE_SPEC=TRANSFORMED_DATA_FEATURE_SPEC)
        return model_inputs_for_training
    # }}}
        # {{{ def build_dnn_and_keras_inputs(self, model_inputs):

    @MyTracePath.inspect_function_execution
    def build_dnn_and_keras_inputs(self, model_inputs):
        """
        Splits model inputs into two: one part goes into the dense
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
            input_name: model_inputs[input_name]
            for input_name in dnn_input_names}
        keras_preprocessing_inputs = {}
        keras_preprocessing_inputs = {
            input_name: model_inputs[input_name]
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
    def get_dataset(self, mydataclass, location):
        """
        Read the tfrecords on disk, get the feature spec stored to disk,
        split the dataset into X, y, batch and make infinite and
        return for training.
        """
        list_of_transformed_files = glob.glob(
            os.path.join(
                self.WORKING_DIRECTORY,
                location,
                location) + '*')
        transformed_ds = tf.data.TFRecordDataset(
            filenames=list_of_transformed_files)
        feature_spec = self.get_feature_spec(location=location)
        transformed_ds = transformed_ds.map(
            lambda X: tf.io.parse_single_example(
                serialized=X, features=feature_spec))
        transformed_ds = transformed_ds.map(
            lambda X: self.map_features_and_labels(
                mydataclass, X))

        # ipdb()
        transformed_ds = transformed_ds.batch(
            batch_size=mydataclass.BATCH_SIZE).repeat()
        return transformed_ds

        # }}}
        # {{{
    @MyTracePath.inspect_function_execution
    def get_subset(self, some_dict, some_columns):
        getter = itemgetter(*some_columns)
        return dict(zip(some_columns, getter(some_dict)))
        # }}}
        # {{{ get transformed dataset with subset

    @MyTracePath.inspect_function_execution
    def get_dataset_with_subset(self, mydataclass, location, columns):
        """
        Read the tfrecords on disk, get the feature spec stored to disk,
        split the dataset into X, y, batch and make infinite and
        return for training.
        """
        list_of_transformed_files = glob.glob(
            os.path.join(
                self.WORKING_DIRECTORY,
                location,
                location) + '*')
        transformed_ds = tf.data.TFRecordDataset(
            filenames=list_of_transformed_files)
        feature_spec = self.get_feature_spec(location=location)
        transformed_ds = transformed_ds.map(
            lambda X: tf.io.parse_single_example(
                serialized=X, features=feature_spec))
        transformed_ds = transformed_ds.map(lambda X: self.get_subset(
            some_dict=X, some_columns=(list(columns) + 
                                       [mydataclass.LABEL_COLUMN])))
        transformed_ds = transformed_ds.map(
            lambda X: self.map_features_and_labels(
                mydataclass, X))

        # ipdb()
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
        2. Creating an input layer for the model and splitting out 
            non-embedding section.
        3. Creates a transformed dataset and trains the model on it.
        """
        task_name = 'train_non_embedding_model'
        self.perform_prerequisites(task_name=task_name)
        location = mydataclass.TRANSFORMED_DATA_LOCATION
        all_inputs = self.build_model_inputs(mydataclass)
        dnn_inputs, keras_preprocessing_inputs = self.build_dnn_and_keras_inputs(
            all_inputs)
        non_embedding_model = self.build_non_embedding_model(
            dnn_inputs, )
        print(f"dnn input keys are {dnn_inputs.keys()}.")
        transformed_ds = self.get_dataset_with_subset(
            mydataclass=mydataclass, location=location,
            columns=dnn_inputs.keys())
        non_embedding_model.fit(transformed_ds, epochs=8, steps_per_epoch=64)
        self.task_completed['train_non_embedding_model'] = True
        return non_embedding_model
        # }}}
        # {{{ Build embedding model

    @MyTracePath.inspect_function_execution
    def build_embedding_model(self, mydataclass):
        """
        Build, compile, and return a model that uses keras preprocessing
        layers in conjunction with the preprocessing already done in tensorflow
        transform.
        """
        all_inputs = self.build_model_inputs(mydataclass)
        dnn_inputs, keras_preprocessing_inputs = self.build_dnn_and_keras_inputs(
            all_inputs)
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
        new_model = tf.keras.Model(all_inputs, output)
        new_model.compile(optimizer='adam', loss='mse', metrics=['mse'])
        return new_model

        # }}}
        # {{{ Train embedding model

    @MyTracePath.inspect_function_execution
    def train_embedding_model(self, mydataclass):
        """
        Task function: trains a model with embeddings by:

        1. Ensuring pre-requisites are completed
        2. Creating an input layer for the model for both dense and 
            preprocessing layers.
        3. Creates a transformed dataset and trains the model on it.
        """
        task_name = 'train_embedding_model'
        self.perform_prerequisites(task_name)
        location = mydataclass.TRANSFORMED_DATA_LOCATION
        all_inputs = self.build_model_inputs(mydataclass)
        dnn_inputs, keras_preprocessing_inputs = self.build_dnn_and_keras_inputs(
            all_inputs)
        embedding_model = self.build_embedding_model(
            mydataclass)
        transformed_ds = self.get_dataset(
            mydataclass=mydataclass, location=location)
        embedding_model.fit(transformed_ds, epochs=1, steps_per_epoch=64)
        self.task_completed['train_embedding_model'] = True
        return embedding_model
        # }}}
        # {{{ end to end model non_embedding

    @MyTracePath.inspect_function_execution
    def build_end_to_end_model_non_embedding(self, mydataclass,
                                             raw_inputs,
                                             location,
                                             new_model):
        """
        Connect the raw inputs to the trained model by using the tft_layer
        to transform raw data to a format that the model preprocessing pipeline
        already knows how to handle.
        """
        tft_transform_output = self.get_tft_transform_output(
            location)
        tft_layer = tft_transform_output.transform_features_layer()
        x = tft_layer(raw_inputs)
        outputs = new_model(x)
        end_to_end_model = tf.keras.Model(raw_inputs, outputs)
        return end_to_end_model

    # }}}
        # {{{ end to end model

    @MyTracePath.inspect_function_execution
    def build_end_to_end_model(self,
                               raw_inputs,
                               location,
                               new_model):
        """
        Connect the raw inputs to the trained model by using the tft_layer
        to transform raw data to a format that the model preprocessing pipeline
        already knows how to handle.
        """
        tft_transform_output = self.get_tft_transform_output(
            location)
        tft_layer = tft_transform_output.transform_features_layer()
        x = tft_layer(raw_inputs)
        outputs = new_model(x)
        end_to_end_model = tf.keras.Model(raw_inputs, outputs)
        return end_to_end_model

    # }}}
# {{{ Train and predict non embedding model

    @MyTracePath.inspect_function_execution
    def train_and_predict_non_embedding_model(self, mydataclass):
        """
        Task function: trains a model with embeddings by:

        1. Ensuring pre-requisites are completed
        2. Creating an input layer for the model for both dense and 
            preprocessing layers.
        3. Creates a transformed dataset and trains the model on it.
        4. Attaches the raw inputs to make an end-to-end graph that includes
           everything
        5. Takes a single raw input sample and does a prediction on it
        """
        task_name = 'train_and_predict_non_embedding_model'
        self.perform_prerequisites(task_name)
        raw_inputs = self.build_raw_inputs(mydataclass)
        location = mydataclass.TRANSFORMED_DATA_LOCATION
        single_original_batch_example = self.get_single_batched_example(
            location=mydataclass.RAW_DATA_LOCATION)
        non_embedding_model = self.train_non_embedding_model(
            mydataclass=mydataclass)
        end_to_end_model = self.build_end_to_end_model_non_embedding(
            raw_inputs=raw_inputs,
            location=location,
            new_model=non_embedding_model, mydataclass=mydataclass)
        print(end_to_end_model.predict(single_original_batch_example))
        self.task_completed['train_and_predict_non_embedding_model'] = True
# }}}
# {{{ Train and predict embedding model

    @MyTracePath.inspect_function_execution
    def train_and_predict_embedding_model(self, mydataclass):
        """
        Task function: trains a model with embeddings by:

        1. Ensuring pre-requisites are completed
        2. Creating an input layer for the model for both dense and 
            preprocessing layers.
        3. Creates a transformed dataset and trains the model on it.
        4. Attaches the raw inputs to make an end-to-end graph that includes
           everything
        5. Takes a single raw input sample and does a prediction on it
        """
        task_name = 'train_and_predict_embedding_model'
        self.perform_prerequisites(task_name)
        raw_inputs = self.build_raw_inputs(mydataclass)
        location = mydataclass.TRANSFORMED_DATA_LOCATION
        single_original_batch_example = self.get_single_batched_example(
            location=mydataclass.RAW_DATA_LOCATION)
        embedding_model = self.train_embedding_model(mydataclass=mydataclass)
        end_to_end_model = self.build_end_to_end_model(
            raw_inputs=raw_inputs,
            location=location,
            new_model=embedding_model)
        print(end_to_end_model.predict(single_original_batch_example))
        self.task_completed['train_and_predict_embedding_model'] = True
# }}}
# {{{ Perform task def perform_task(self, task_name):

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
                    dict(self.task_completed),
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
# {{{ Main function def main(args):


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
    TRANSFORMED_DATA_LOCATION = 'transformed_tfrecords'
    RAW_DATA_LOCATION = 'raw_tfrecords'
    # }}}
# {{{ Instantiating instances
    my_taxicab_data = MLMetaData(
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
        TRANSFORMED_DATA_LOCATION=TRANSFORMED_DATA_LOCATION,
        RAW_DATA_LOCATION=RAW_DATA_LOCATION)
    my_tasks = Task(
        WORKING_DIRECTORY=WORKING_DIRECTORY,
        # task_completed = {},
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
    del(TRANSFORMED_DATA_LOCATION)
    del(WORKING_DIRECTORY)
    del(RAW_DATA_LOCATION)
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
        # cannot use the below in tft as managing those learned values needs 
        # to be
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
    my_tasks.task_dictionary["preprocess_raw_data"] = partial(
        my_tasks.preprocess_raw_data, mydataclass=my_taxicab_data,
        )
    new_preprocessing_fn = partial(
        preprocessing_fn,
        mydataclass=my_taxicab_data)

    my_tasks.task_dictionary["transform_raw_data"] = partial(
        my_tasks.transform_raw_data,
        preprocessing_fn=new_preprocessing_fn,
        mydataclass=my_taxicab_data)
    my_tasks.task_dictionary["view_original_sample_data"] = partial(
        my_tasks.view_original_sample_data, mydataclass=my_taxicab_data)
    my_tasks.task_dictionary["view_transformed_sample_data"] = partial(
        my_tasks.view_transformed_sample_data, mydataclass=my_taxicab_data)
    my_tasks.task_dictionary["train_non_embedding_model"] = partial(
        my_tasks.train_non_embedding_model, mydataclass=my_taxicab_data)
    my_tasks.task_dictionary["train_embedding_model"] = partial(
        my_tasks.train_embedding_model, mydataclass=my_taxicab_data)
    my_tasks.task_dictionary["train_and_predict_non_embedding_model"] = partial(
        my_tasks.train_and_predict_non_embedding_model, mydataclass=my_taxicab_data)
    my_tasks.task_dictionary["train_and_predict_embedding_model"] = partial(
        my_tasks.train_and_predict_embedding_model, mydataclass=my_taxicab_data)

    my_tasks.task_dag = {key: [] for key in my_tasks.task_dictionary.keys()}
    my_tasks.task_dag['view_original_sample_data'].append(
        'preprocess_raw_data')
    my_tasks.task_dag['view_transformed_sample_data'].append(
        'preprocess_raw_data')
    my_tasks.task_dag['view_transformed_sample_data'].append(
        'transform_raw_data')
    my_tasks.task_dag['train_non_embedding_model'].append(
        'transform_raw_data')
    my_tasks.task_dag['train_embedding_model'].append('transform_raw_data')
    my_tasks.task_dag['train_and_predict_non_embedding_model'].append(
        'preprocess_raw_data')
    my_tasks.task_dag['train_and_predict_non_embedding_model'].append(
        'transform_raw_data')
    my_tasks.task_dag['train_and_predict_embedding_model'].append(
        'preprocess_raw_data')
    my_tasks.task_dag['train_and_predict_embedding_model'].append(
        'transform_raw_data')
    # }}}
    # {{{ Task execution and closeout
    if os.path.isfile(my_tasks.TASK_STATE_FILEPATH):
        with open(my_tasks.TASK_STATE_FILEPATH, 'rb') as task_state_file:
            # my_taxicab_data.task_completed  = pickle.load(
            #     task_state_file)
            place_holder_dict = pickle.load(task_state_file)
            my_tasks.task_completed.update(place_holder_dict)
    for task in args.tasks:
        if task not in my_tasks.valid_tasks:
            print(f"Invalid task {task}."
                  f"Pick tasks from {list(my_tasks.valid_tasks)}")

            continue
        else:
            my_tasks.perform_task(task)
    if args.visualization_filename:
        MyTracePath.construct_graph()
        MyTracePath.draw_graph(filename=args.visualization_filename[0])
    if args.print_performance:
        MyTracePath.display_performance()
    my_tasks.closeout_task()
    # }}}

    # }}}

# {{{ Allow importing into other programs


if __name__ == '__main__':
    # We no longer provide an entry point from here.
    print("Please access via tft_tasks_cli.py or by importing tft_tasks.")
    """
    Gets input from argparse, calls main with those inputs, and
    produces the visualization if requested by the user
    """
# }}}
