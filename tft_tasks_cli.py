

#!/usr/bin/env python
# coding: utf-8
# {{{ Imports
# Goal: Implement taxicab fare prediction with a tft pipeline with safety
# checks and a simpler flow.
import tensorflow_metadata
from typing import List, Dict, ClassVar, FrozenSet, Set
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
from tft_tasks import main as tft_tasks_main
from tft_tasks import Task
# print(tf.version.VERSION)
# }}}
# {{{ Parser function
# @MyTracePath.inspect_function_execution
# }}}
# {{{ Main function


def main():
    def get_args():
        parser = argparse.ArgumentParser(
            prog="tft_tasks_cli.py",
            description="A task based approach to using tensorflow transform.")
        parser.add_argument(
            '--task',
            dest='tasks',
            action='append',
            required=True,
            help=f'Pick tasks from {set(Task.valid_tasks)}')
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
    args = get_args()
    tft_tasks_main(args)
    # if args.visualization_filename:
    #     MyTracePath.construct_graph()
    #     MyTracePath.draw_graph(filename=args.visualization_filename[0])
    # if args.print_performance:
    #     MyTracePath.display_performance()
    # {{{ Task execution and closeout
    # if os.path.isfile(my_tasks.TASK_STATE_FILEPATH):
    #     with open(my_tasks.TASK_STATE_FILEPATH, 'rb') as task_state_file:
    #         # my_taxicab_data.task_state_dictionary = pickle.load(
    #         #     task_state_file)
    #         place_holder_dict = pickle.load(task_state_file)
    #         my_tasks.task_state_dictionary.update(place_holder_dict)
    # for task in args.tasks:
    #     print(task)
    # }}}

    # }}}
# {{{ Allow importing into other programs


if __name__ == '__main__':
    """
    Gets input from argparse, calls main with those inputs, and
    produces the visualization if requested by the user
    """
    main()
    # if args.visualization_filename:
    #     MyTracePath.construct_graph()
    #     MyTracePath.draw_graph(filename=args.visualization_filename[0])
    # if args.print_performance:
    #     MyTracePath.display_performance()
# }}}
