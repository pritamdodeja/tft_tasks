#!/usr/bin/env python
# coding: utf-8
# {{{ Imports
# Goal: Implement taxicab fare prediction with a tft pipeline with safety
# checks and a simpler flow.
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
MyTracePath = TracePath(instrument=True, name="MyTracePath")
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'  # or any {'0', '1', '2'}
# set TF error log verbosity
logger = logging.getLogger("tensorflow").setLevel(logging.INFO)
# print(tf.version.VERSION)


# }}}
# {{{ Constants
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

DAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]

NBUCKETS = 10

test_file_path = './data/taxi-valid.csv'
train_file_path = './data/taxi-train_toy.csv'
WORKING_DIRECTORY = './working_area'


# We need to create the schema, for which we need the raw data feature spec
RAW_DATA_DICTIONARY = {
    key: value for (
        key,
        value) in zip(
        CSV_COLUMNS,
        ALL_DTYPES)}

RAW_DATA_FEATURE_SPEC = {
    raw_feature_name: tf.io.FixedLenFeature(
        shape=(1,),
        dtype=RAW_DATA_DICTIONARY[raw_feature_name])
    for raw_feature_name in CSV_COLUMNS}

# _SCHEMA = dataset_metadata.DatasetMetadata(
#     schema_utils.schema_from_feature_spec(RAW_DATA_FEATURE_SPEC)).schema
_SCHEMA = schema_utils.schema_from_feature_spec(RAW_DATA_FEATURE_SPEC)

BATCH_SIZE = 8
PREFIX_STRING = 'transformed_tfrecords'
task_state_file_name = 'task_state.pkl'
task_state_filepath = os.path.join(WORKING_DIRECTORY, task_state_file_name)


def return_none():
    return None


if os.path.isfile(task_state_filepath):
    with open(task_state_filepath, 'rb') as task_state_file:
        task_state_dictionary = pickle.load(task_state_file)
else:
    task_state_dictionary = collections.defaultdict(return_none)

# }}}
# {{{ no preprocessing fn
# @MyTracePath.inspect_function_execution


def no_preprocessing_fn(inputs):
    """Preprocess input columns into transformed columns."""
    # Since we are modifying some features and leaving others unchanged, we
    # start by setting `outputs` to a copy of `inputs.
    outputs = inputs.copy()
    return outputs
# }}}
# {{{ Time since 1970 function


@tf.function
def fn_seconds_since_1970(ts_in):
    """Compute the seconds since 1970 given a datetime string"""
    seconds_since_1970 = tfa.text.parse_time(
        ts_in, "%Y-%m-%d %H:%M:%S %Z", output_unit="SECOND"
        )
    seconds_since_1970 = tf.cast(seconds_since_1970, tf.float32)
    return seconds_since_1970
# }}}
# {{{ preprocessing fn


# @MyTracePath.inspect_function_execution
def preprocessing_fn(inputs):
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
    lat_lon_buckets = [bin_edge / NBUCKETS for bin_edge in range(0, NBUCKETS)]

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
        output_mode='one_hot', num_bins=NBUCKETS**2, name='hash_pickup_crossing_layer')
    transformed['pickup_location'] = hash_pickup_crossing_layer(
        (transformed['bucketed_pickup_latitude'],
         transformed['bucketed_pickup_longitude']))
    hash_dropoff_crossing_layer = tf.keras.layers.experimental.preprocessing.HashedCrossing(
        output_mode='one_hot', num_bins=NBUCKETS**2, name='hash_dropoff_crossing_layer')
    transformed['dropoff_location'] = hash_dropoff_crossing_layer(
        (transformed['bucketed_dropoff_latitude'],
         transformed['bucketed_dropoff_longitude']))

    hash_pickup_crossing_layer_intermediary = tf.keras.layers.experimental.preprocessing.HashedCrossing(
        output_mode='int', num_bins=NBUCKETS**2, )
    hashed_pickup_intermediary = hash_pickup_crossing_layer_intermediary(
        (transformed['bucketed_pickup_longitude'],
         transformed['bucketed_pickup_latitude']))
    hash_dropoff_crossing_layer_intermediary = tf.keras.layers.experimental.preprocessing.HashedCrossing(
        output_mode='int', num_bins=NBUCKETS**2, )
    hashed_dropoff_intermediary = hash_dropoff_crossing_layer_intermediary(
        (transformed['bucketed_dropoff_longitude'],
         transformed['bucketed_dropoff_latitude']))

    hash_trip_crossing_layer = tf.keras.layers.experimental.preprocessing.HashedCrossing(
        output_mode='one_hot', num_bins=NBUCKETS ** 3, name="hash_trip_crossing_layer")
    transformed['hashed_trip'] = hash_trip_crossing_layer(
        (hashed_pickup_intermediary,
         hashed_dropoff_intermediary))

    seconds_since_1970 = fn_seconds_since_1970(inputs['pickup_datetime'])
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
        output_mode='int', num_bins=NBUCKETS ** 3)
    hashed_trip_intermediary = hash_trip_crossing_layer_intermediary(
        (hashed_pickup_intermediary, hashed_dropoff_intermediary))

    hash_trip_and_time_layer = tf.keras.layers.experimental.preprocessing.HashedCrossing(
        output_mode='one_hot', num_bins=(NBUCKETS ** 3) * 4, name='hash_trip_and_time_layer')
    transformed['hashed_trip_and_time'] = hash_trip_and_time_layer(
        (hashed_trip_intermediary, hour_of_day_of_week_intermediary))
    return transformed


# }}}
# {{{ pipeline function


@MyTracePath.inspect_function_execution
def pipeline_function(prefix_string, preprocessing_fn):
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
                column_names=CSV_COLUMNS,
                schema=_SCHEMA)

            # Read in raw data and convert using CSV TFXIO.  Note that we apply
            # some Beam transformations here, which will not be encoded in the
            # TF graph since we don't do the from within tf.Transform's methods
            # (AnalyzeDataset, TransformDataset etc.).  These transformations
            #  are just to get data into a format that the CSV TFXIO can read,
            #  in particular removing spaces after commas.
            raw_data = (
                pipeline | 'ReadTrainData' >> beam.io.ReadFromText(
                    file_pattern=train_file_path,
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
            tfrecord_directory = os.path.join(WORKING_DIRECTORY, prefix_string)
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
# {{{ Get tft_transform_output


@MyTracePath.inspect_function_execution
def get_tft_transform_output(working_directory, prefix_string):
    """Get tft transform output from a specified location"""
    transform_fn_output_directory = os.path.join(
        working_directory, prefix_string, 'transform_output')
    tft_transform_output = tft.TFTransformOutput(transform_fn_output_directory)
    return tft_transform_output

# }}}
# {{{ Get single raw example


@MyTracePath.inspect_function_execution
def get_single_example(working_directory, prefix_string):
    """Produce a single example batch of raw data."""
    list_of_original_files = glob.glob(os.path.join(working_directory,
                                                    prefix_string,
                                       prefix_string) + '*')
    my_tf_ds = tf.data.TFRecordDataset(list_of_original_files)
    example_string = my_tf_ds.take(1).get_single_element()
    single_example = tf.io.parse_single_example(serialized=example_string,
                                                features=RAW_DATA_FEATURE_SPEC)
    single_example_batched = {key: tf.expand_dims(
        single_example[key], -1) for key in single_example.keys()}
    return single_example_batched

# }}}
# {{{ inspect_example function


@MyTracePath.inspect_function_execution
def inspect_example(dictionary_of_tensors):
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
# {{{ task functions
# Pre-requisite: raw data is there


@MyTracePath.inspect_function_execution
def write_raw_tfrecords():
    """
    Task function: Create tfrecords from raw data using an identity
    function.
    """
    if task_state_dictionary["write_raw_tfrecords"] is None:
        original_prefix_string = 'raw_tfrecords'
        pipeline_function(prefix_string=original_prefix_string,
                          preprocessing_fn=no_preprocessing_fn)
        task_state_dictionary["write_raw_tfrecords"] = True


# Pre-requisite: raw data is there
@MyTracePath.inspect_function_execution
def transform_tfrecords():
    """
    Task function: Call the pipeline function with transform_tfrecords
    prefix and provide the preprocessing_fn, which creates transformed
    tfrecords.
    """
    prefix_string = PREFIX_STRING
    pipeline_function(prefix_string=prefix_string,
                      preprocessing_fn=preprocessing_fn)
    task_state_dictionary["transform_tfrecords"] = True


# Pre-requisite: None
@MyTracePath.inspect_function_execution
def clean_directory():
    """Task function: cleans the working directory."""
    if os.path.exists(WORKING_DIRECTORY) and os.path.isdir(WORKING_DIRECTORY):
        shutil.rmtree(WORKING_DIRECTORY)
    assert(not os.path.exists(WORKING_DIRECTORY))
    print(f"Directory {WORKING_DIRECTORY} no longer exists.")
    for key in task_state_dictionary.keys():
        task_state_dictionary[key] = None


# Pre-requisite: Data has already been pre-processed
@MyTracePath.inspect_function_execution
def train_non_embedding_model():
    """
    Task function: trains a model without embeddings by:

    1. Ensuring pre-requisites are completed
    2. Creating an input layer for the model and splitting out non-embedding
       section.
    3. Creates a transformed dataset and trains the model on it.
    """
    task_prerequisites = task_dag['train_non_embedding_model']
    if not check_prerequisites(task_prerequisites):
        # Do pre-reqs
        for task in task_prerequisites:
            if not task_state_dictionary[task]:
                perform_task(task)
    assert(check_prerequisites(task_prerequisites))
    # build_raw_inputs(RAW_DATA_FEATURE_SPEC)
    prefix_string = PREFIX_STRING
    tft_transform_output = get_tft_transform_output(
        WORKING_DIRECTORY, prefix_string)
    TRANSFORMED_DATA_FEATURE_SPEC = tft_transform_output.transformed_feature_spec()
    TRANSFORMED_DATA_FEATURE_SPEC.pop(LABEL_COLUMN)
    transformed_inputs = build_transformed_inputs(
        TRANSFORMED_DATA_FEATURE_SPEC)
    dnn_inputs, keras_preprocessing_inputs = build_dnn_and_keras_inputs(
        transformed_inputs)
    non_embedding_model = build_non_embedding_model(
        dnn_inputs, )
    transformed_ds = get_transformed_dataset(
        WORKING_DIRECTORY, prefix_string, batch_size=BATCH_SIZE)
    non_embedding_model.fit(transformed_ds, epochs=1, steps_per_epoch=64)
    task_state_dictionary['train_non_embedding_model'] = True


# Pre-requisite: Data has already been pre-processed
@MyTracePath.inspect_function_execution
def train_embedding_model():
    """
    Task function: trains a model with embeddings by:

    1. Ensuring pre-requisites are completed
    2. Creating an input layer for the model for both dense and preprocessing
       layers.
    3. Creates a transformed dataset and trains the model on it.
    """
    task_prerequisites = task_dag['train_embedding_model']
    if not check_prerequisites(task_prerequisites):
        # Do pre-reqs
        for task in task_prerequisites:
            if not task_state_dictionary[task]:
                perform_task(task)
    assert(check_prerequisites(task_prerequisites))
    build_raw_inputs(RAW_DATA_FEATURE_SPEC)
    prefix_string = PREFIX_STRING
    tft_transform_output = get_tft_transform_output(
        WORKING_DIRECTORY, prefix_string)
    TRANSFORMED_DATA_FEATURE_SPEC = tft_transform_output.transformed_feature_spec()
    TRANSFORMED_DATA_FEATURE_SPEC.pop(LABEL_COLUMN)
    transformed_inputs = build_transformed_inputs(
        TRANSFORMED_DATA_FEATURE_SPEC)
    dnn_inputs, keras_preprocessing_inputs = build_dnn_and_keras_inputs(
        transformed_inputs)
    embedding_model = build_embedding_model(transformed_inputs)
    transformed_ds = get_transformed_dataset(
        WORKING_DIRECTORY, prefix_string, batch_size=BATCH_SIZE)
    embedding_model.fit(transformed_ds, epochs=1, steps_per_epoch=64)
    task_state_dictionary['train_embedding_model'] = True


@MyTracePath.inspect_function_execution
def check_prerequisites(task_prerequisites):
    """Return whether or not all prequisites have been met for a task."""
    prerequisites_met = True
    for task in task_prerequisites:
        prerequisites_met = prerequisites_met and task_state_dictionary[task]
    return prerequisites_met

# Pre-requisite: raw tfrecords have been written


@MyTracePath.inspect_function_execution
def view_original_sample_data():
    """
    Task function: Gets raw input data and shows the data along
    with datatypes and shapes.
    """
    task_prerequisites = task_dag['view_original_sample_data']
    if not check_prerequisites(task_prerequisites):
        # Do pre-reqs
        for task in task_prerequisites:
            if not task_state_dictionary[task]:
                perform_task(task)
    assert(check_prerequisites(task_prerequisites))
    single_original_batch_example = get_single_batched_example(
        WORKING_DIRECTORY, prefix_string='raw_tfrecords')
    print('-' * 80)
    print(single_original_batch_example)
    print('-' * 80)
    inspect_example(single_original_batch_example)


# Pre-requisite: transformed tfrecords have been written
@MyTracePath.inspect_function_execution
def view_transformed_sample_data():
    """
    Take raw input, run it through preprocessing function, and show
    what it started out as, and what it got transformed to.
    """
    task_prerequisites = task_dag['view_transformed_sample_data']
    if not check_prerequisites(task_prerequisites):
        # Do pre-reqs
        for task in task_prerequisites:
            if not task_state_dictionary[task]:
                perform_task(task)
    assert(check_prerequisites(task_prerequisites))
    prefix_string = PREFIX_STRING
    raw_inputs = build_raw_inputs(RAW_DATA_FEATURE_SPEC)
    single_original_batch_example = get_single_batched_example(
        WORKING_DIRECTORY, prefix_string='raw_tfrecords')
    tft_transform_output = get_tft_transform_output(
        WORKING_DIRECTORY, prefix_string)
    raw_to_preprocessing_model = build_raw_to_preprocessing_model(
        raw_inputs, tft_transform_output)
    transformed_batch_example = raw_to_preprocessing_model(
        single_original_batch_example)
    print('-' * 80)
    print(f"Start out with: {single_original_batch_example}")
    print('-' * 80)
    print(f"End up with: {transformed_batch_example}")
    inspect_example(transformed_batch_example)


# Raw tfrecords have been written, data has been preprocessed
@MyTracePath.inspect_function_execution
def train_and_predict_embedding_model():
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
    task_prerequisites = task_dag['train_and_predict_embedding_model']
    if not check_prerequisites(task_prerequisites):
        # Do pre-reqs
        for task in task_prerequisites:
            if not task_state_dictionary[task]:
                perform_task(task)
    assert(check_prerequisites(task_prerequisites))
    raw_inputs = build_raw_inputs(RAW_DATA_FEATURE_SPEC)
    prefix_string = PREFIX_STRING
    single_original_batch_example = get_single_batched_example(
        WORKING_DIRECTORY, prefix_string='raw_tfrecords')
    tft_transform_output = get_tft_transform_output(
        WORKING_DIRECTORY, prefix_string)
    TRANSFORMED_DATA_FEATURE_SPEC = tft_transform_output.transformed_feature_spec()
    TRANSFORMED_DATA_FEATURE_SPEC.pop(LABEL_COLUMN)
    transformed_inputs = build_transformed_inputs(
        TRANSFORMED_DATA_FEATURE_SPEC)
    # dnn_inputs, keras_preprocessing_inputs = build_dnn_and_keras_inputs(
    #     transformed_inputs)
    embedding_model = build_embedding_model(transformed_inputs)
    transformed_ds = get_transformed_dataset(
        WORKING_DIRECTORY, prefix_string, batch_size=BATCH_SIZE)
    embedding_model.fit(transformed_ds, epochs=1, steps_per_epoch=64)
    end_to_end_model = build_end_to_end_model(
        raw_inputs,
        transformed_inputs,
        WORKING_DIRECTORY,
        prefix_string,
        embedding_model)
    print(end_to_end_model.predict(single_original_batch_example))
    task_state_dictionary['train_and_predict_embedding_model'] = True


@MyTracePath.inspect_function_execution
def closeout_task(task_state_dictionary):
    """Persist the task state information to disk"""
    if os.path.exists(WORKING_DIRECTORY) and os.path.isdir(WORKING_DIRECTORY):
        with open(task_state_filepath, 'wb') as task_state_file:
            pickle.dump(
                task_state_dictionary,
                task_state_file,
                protocol=pickle.HIGHEST_PROTOCOL)

    # }}}
# {{{ task dictionary


task_dictionary = {}
task_dictionary['write_raw_tfrecords'] = write_raw_tfrecords
task_dictionary['transform_tfrecords'] = transform_tfrecords
task_dictionary['clean_directory'] = clean_directory
task_dictionary['train_non_embedding_model'] = train_non_embedding_model
task_dictionary['train_embedding_model'] = train_embedding_model
task_dictionary['view_original_sample_data'] = view_original_sample_data
task_dictionary['view_transformed_sample_data'] = view_transformed_sample_data
task_dictionary['train_and_predict_embedding_model'] = train_and_predict_embedding_model
valid_tasks = set(task_dictionary.keys())

task_dag = {key: [] for key in task_dictionary.keys()}
task_dag['view_original_sample_data'].append('write_raw_tfrecords')
task_dag['view_transformed_sample_data'].append('write_raw_tfrecords')
task_dag['view_transformed_sample_data'].append('transform_tfrecords')
task_dag['train_non_embedding_model'].append('transform_tfrecords')
task_dag['train_embedding_model'].append('transform_tfrecords')
task_dag['train_and_predict_embedding_model'].append('write_raw_tfrecords')
task_dag['train_and_predict_embedding_model'].append('transform_tfrecords')

# }}}
# {{{ Perform task


@MyTracePath.inspect_function_execution
def perform_task(task_name):
    """
    Execute task functions stored in the task_dictionary.
    """
    if task_name in valid_tasks:
        task_dictionary[task_name]()
        # }}}
# {{{ Model building functions
# {{{ build raw inputs build_raw_inputs(RAW_DATA_FEATURE_SPEC):


@MyTracePath.inspect_function_execution
def build_raw_inputs(RAW_DATA_FEATURE_SPEC):
    """Produce keras input layer for raw data for end-to-end model"""
    raw_inputs_for_training = {}
    for key, spec in RAW_DATA_FEATURE_SPEC.items():
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
# {{{ Build transformed inputs
# build_transformed_inputs(TRANSFORMED_DATA_FEATURE_SPEC):


@MyTracePath.inspect_function_execution
def build_transformed_inputs(TRANSFORMED_DATA_FEATURE_SPEC):
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
# {{{ Build dnn and keras inputs build_dnn_and_keras_inputs(transformed_inputs)


@MyTracePath.inspect_function_execution
def build_dnn_and_keras_inputs(transformed_inputs):
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
# {{{ Build non embedding model build_non_embedding_model():


@MyTracePath.inspect_function_execution
def build_non_embedding_model(dnn_inputs, ):
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
    non_embedding_model.compile(optimizer='adam', loss='mse', metrics=['mse'])
    return non_embedding_model
# }}}
# {{{ map features and labels function map_features_and_labels(example):


@MyTracePath.inspect_function_execution
def map_features_and_labels(example):
    """Split the label from the features"""
    label = example.pop(LABEL_COLUMN)
    return example, label
# }}}
# {{{ get transformed dataset function
# get_transformed_dataset(WORKING_DIRECTORY, prefix_string, batch_size):


@MyTracePath.inspect_function_execution
def get_transformed_dataset(working_directory, prefix_string, batch_size):
    """
    Read the tfrecords on disk, get the feature spec stored to disk,
    split the dataset into X, y, batch and make infinite and
    return for training.
    """
    list_of_transformed_files = glob.glob(os.path.join(working_directory,
                                                       prefix_string,
                                                       prefix_string) + '*')
    transformed_ds = tf.data.TFRecordDataset(
        filenames=list_of_transformed_files)
    tft_transform_output = get_tft_transform_output(
        working_directory=WORKING_DIRECTORY,
        prefix_string=prefix_string)

    feature_spec = tft_transform_output.transformed_feature_spec().copy()
    transformed_ds = transformed_ds.map(
        lambda X: tf.io.parse_single_example(
            serialized=X, features=feature_spec))
    # transformed_ds = transformed_ds.map(lambda X: (X, X.pop("fare_amount")))
    transformed_ds = transformed_ds.map(map_features_and_labels)
    transformed_ds = transformed_ds.batch(batch_size=BATCH_SIZE).repeat()
    return transformed_ds
# }}}
# {{{ Build keras preprocessing layers
# build_keras_preprocessing_layers(keras_preprocessing_inputs):


@MyTracePath.inspect_function_execution
def build_keras_preprocessing_layers(keras_preprocessing_inputs):
    """
    Construct keras preprocessing layers that will feed into the embedding
    layers in the model.
    """
    bucketed_pickup_longitude_intermediary = keras_preprocessing_inputs[
        'bucketed_pickup_longitude']
    bucketed_pickup_latitude_intermediary = keras_preprocessing_inputs[
        'bucketed_pickup_latitude']
    bucketed_dropoff_longitude_intermediary = keras_preprocessing_inputs[
        'bucketed_dropoff_longitude']
    bucketed_dropoff_latitude_intermediary = keras_preprocessing_inputs[
        'bucketed_dropoff_latitude']
    hash_pickup_crossing_layer_intermediary = tf.keras.layers.experimental.preprocessing.HashedCrossing(
        output_mode='int', num_bins=NBUCKETS**2, )
    hashed_pickup_intermediary = hash_pickup_crossing_layer_intermediary(
        (bucketed_pickup_longitude_intermediary, bucketed_pickup_latitude_intermediary))
    hash_dropoff_crossing_layer_intermediary = tf.keras.layers.experimental.preprocessing.HashedCrossing(
        output_mode='int', num_bins=NBUCKETS**2, )
    hashed_dropoff_intermediary = hash_dropoff_crossing_layer_intermediary(
        (bucketed_dropoff_longitude_intermediary, bucketed_dropoff_latitude_intermediary))
    hash_trip_crossing_layer = tf.keras.layers.experimental.preprocessing.HashedCrossing(
        output_mode='one_hot', num_bins=NBUCKETS ** 3, name="hash_trip_crossing_layer")
    hashed_trip = hash_trip_crossing_layer(
        (hashed_pickup_intermediary,
         hashed_dropoff_intermediary))
    return hashed_trip
# }}}
# {{{ Build raw to preprocessing model build_raw_to_preprocessing_model(
#     raw_inputs, tft_layer):


@MyTracePath.inspect_function_execution
def build_raw_to_preprocessing_model(raw_inputs, tft_transform_output):
    """
    Produce a model that transforms raw inputs into the non-embedding
    component of the model. This uses the tft_layer provided by tft.

    """
    tft_layer = tft_transform_output.transform_features_layer()
    return tf.keras.Model(raw_inputs, tft_layer(raw_inputs))
# }}}
# {{{ Build preprocessing model build_preprocessing_model(
#     transformed_inputs, dnn_inputs, hashed_trip):


@MyTracePath.inspect_function_execution
def build_preprocessing_model(transformed_inputs, dnn_inputs, hashed_trip):
    stacked_inputs = tf.concat(tf.nest.flatten(dnn_inputs), axis=1)
    return tf.keras.Model(
        transformed_inputs, tf.concat(
            [stacked_inputs, hashed_trip],
            axis=1),)
# }}}
# {{{ Build model with embeddings build_embedding_model(transformed_inputs):


@MyTracePath.inspect_function_execution
def build_embedding_model(transformed_inputs):
    """
    Build, compile, and return a model that uses keras preprocessing
    layers in conjunction with the preprocessing already done in tensorflow
    transform.
    """
    dnn_inputs, keras_preprocessing_inputs = build_dnn_and_keras_inputs(
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
        output_mode='int', num_bins=NBUCKETS**2, )
    hashed_pickup_intermediary = hash_pickup_crossing_layer_intermediary(
        (bucketed_pickup_longitude_intermediary, bucketed_pickup_latitude_intermediary))
    hash_dropoff_crossing_layer_intermediary = tf.keras.layers.experimental.preprocessing.HashedCrossing(
        output_mode='int', num_bins=NBUCKETS**2, )
    hashed_dropoff_intermediary = hash_dropoff_crossing_layer_intermediary(
        (bucketed_dropoff_longitude_intermediary, bucketed_dropoff_latitude_intermediary))
    hash_trip_crossing_layer = tf.keras.layers.experimental.preprocessing.HashedCrossing(
        output_mode='one_hot', num_bins=NBUCKETS ** 3, name="hash_trip_crossing_layer")
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
    # transformed_inputs.pop(LABEL_COLUMN)
    # transformed_input = tft_layer(raw_inputs)
    trip_locations_embedding_layer = tf.keras.layers.Embedding(
        input_dim=NBUCKETS ** 3, output_dim=int(NBUCKETS ** 1.5),
        name="trip_locations_embedding_layer")
    trip_embedding_layer = tf.keras.layers.Embedding(
        input_dim=(NBUCKETS ** 3) * 4, output_dim=int(NBUCKETS ** 1.5),
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
# {{{ build end to end model


@MyTracePath.inspect_function_execution
def build_end_to_end_model(
        raw_inputs,
        transformed_inputs,
        working_directory,
        prefix_string,
        new_model):
    """
    Connect the raw inputs to the trained model by using the tft_layer
    to transform raw data to a format that the model preprocessing pipeline
    already knows how to handle.
    """
    tft_transform_output = get_tft_transform_output(
        working_directory, prefix_string)
    tft_layer = tft_transform_output.transform_features_layer()
    x = tft_layer(raw_inputs)
    outputs = new_model(x)
    end_to_end_model = tf.keras.Model(raw_inputs, outputs)
    return end_to_end_model
# }}}
# {{{ Get single batched example


@MyTracePath.inspect_function_execution
def get_single_batched_example(working_directory, prefix_string):
    """
    Get a single example by reading from tfrecords and interpreting
    using the feature spec stored by tensorflow transform.
    """
    list_of_tfrecord_files = glob.glob(
        os.path.join(
            working_directory,
            prefix_string,
            prefix_string + '*'))
    dataset = tf.data.TFRecordDataset(list_of_tfrecord_files)
    tft_transform_output = get_tft_transform_output(
        working_directory, prefix_string)
    feature_spec = tft_transform_output.transformed_feature_spec()
    # tft_layer = tft_transform_output.transform_features_layer()
    example_string = dataset.take(1).get_single_element()
    single_example = tf.io.parse_single_example(
        serialized=example_string, features=feature_spec)
    single_example_batched = {key: tf.reshape(
        single_example[key], (1, -1)) for key in single_example.keys()}
    return single_example_batched


# }}}
# }}}
# {{{ Parser function
# @MyTracePath.inspect_function_execution
def get_args():
    usage_string = '''\

A task based approach to using tensorflow transform.

optional arguments:
  -h, --help            show this help message and exit
  --task TASKS          Pick tasks from {'train_embedding_model',
                        'write_raw_tfrecords', 'transform_tfrecords',
                        'train_and_predict_embedding_model',
                        'train_non_embedding_model',
                        'view_original_sample_data',
                        'clean_directory', 'view_transformed_sample_data'}
  --visualize_tasks VISUALIZATION_FILENAME
                        Specify the filename to visualize the execution of tft
                        tasks(e.g. mlops_pipeline.svg)'''
    parser = argparse.ArgumentParser(
        prog="tft_tasks.py", usage=usage_string,
        description="A task based approach to using tensorflow transform.")
    parser.add_argument(
        '--task',
        dest='tasks',
        action='append',
        required=True,
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
    """
    Manages the task state information and calls the various tasks given
    input from argparse
    """
    if os.path.isfile(task_state_filepath):
        with open(task_state_filepath, 'rb') as task_state_file:
            task_state_dictionary = pickle.load(task_state_file)
    else:
        task_state_dictionary = collections.defaultdict(return_none)
    for task in args.tasks:
        if task not in valid_tasks:
            print(f"Invalid task {task}")
            continue
        else:
            perform_task(task)
    closeout_task(task_state_dictionary=task_state_dictionary)
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
