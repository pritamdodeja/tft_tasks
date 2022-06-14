#!/usr/bin/env python
# coding: utf-8
# {{{ Imports
import argparse
from tft_tasks import main as tft_tasks_main
from tft_tasks import Task
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

    # }}}
# {{{ Allow importing into other programs


if __name__ == '__main__':
    """
    Gets input from argparse, calls main with those inputs, and
    produces the visualization if requested by the user
    """
    main()
# }}}
