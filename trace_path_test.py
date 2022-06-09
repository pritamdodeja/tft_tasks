# {{{ Imports
import unittest
from trace_path import TracePath
import pygraphviz as pgv
import os
import tempfile
# }}}
# {{{ Test Constructor


class TestTracePathConstructor(unittest.TestCase):
    def setUp(self):
        self.TestTracePath = TracePath(name="TestTracePath")
        self.TestTracePathInstrumentationDisabled = TracePath(
            name="TestTracePathInstrumentationDisabled", instrument=False)

    def test_constructor(self):
        self.assertEqual(len(self.TestTracePath.function_mapping_list), 0)
        self.assertEqual(self.TestTracePath.graph.number_of_nodes(), 0)
        self.assertTrue(isinstance(self.TestTracePath, TracePath))
        self.assertTrue(self.TestTracePath.instrument, True)
        self.assertEqual(
            self.TestTracePathInstrumentationDisabled.instrument, False)
        self.assertTrue(
            isinstance(
                self.TestTracePath.function_mapping_list,
                list))
        self.assertTrue(
            isinstance(
                self.TestTracePath.function_measuring_list,
                list))
        self.assertTrue(isinstance(self.TestTracePath.graph, pgv.AGraph))

# }}}
# {{{ Test Instrumentation Decorator


class TestTracePathInspectDecorator(unittest.TestCase):
    # {{{ TracePath Setup
    def setUp(self):
        self.EmptyTestTracePath = TracePath(
            name="EmptyTestTracePath", instrument=True)
        self.TestTracePath = TracePath(name="TestTracePath", instrument=True)
        self.TestTracePath.construct_graph()

        @self.TestTracePath.inspect_function_execution
        def myfunc(x, y="the_moon"):
            return x
        # Check if function invocations captured properly
        myfunc(2, y="the_sun")

        @self.TestTracePath.inspect_function_execution
        def newfunc(x):
            return x
        self.EmptyTestTracePath.construct_graph()
        # Instrument another function and check if we don't change behavior
        # and we capture arguments and execution time
        newfunc(2)
        self.empty_graph_file_name = tempfile.mktemp(suffix='.svg')
        self.graph_file_name = tempfile.mktemp(suffix='.svg')
        # }}}
# {{{ Test Empty Case

    def test_inspect_function_empty_execution(self):
        # Check if empty graph handled properly
        self.assertEqual(self.EmptyTestTracePath.graph.number_of_nodes(), 0)
        self.assertEqual(self.EmptyTestTracePath.graph.number_of_edges(), 0)
        # Instrument a function
        # }}}
    # {{{ Test Non Empty Case


def test_inspect_function_non_empty_execution(self):
    # self.assertEqual(self.myfunc(2, y="the_sun"), 2)
    # Check if function invocations captured properly
    self.assertEqual(len(self.TestTracePath.function_mapping_list), 2)
    self.assertEqual(len(self.TestTracePath.function_measuring_list), 2)
    self.assertEqual(
        self.TestTracePath.function_mapping_list[0].called,
        'myfunc')
    self.assertEqual(
        self.TestTracePath.function_mapping_list[0].caller,
        'setUp')
    self.assertEqual(
        str(self.TestTracePath.function_mapping_list[0].args),
        '(2,)')
    self.assertEqual(
        self.TestTracePath.function_mapping_list[0].kwargs, {
            'y': 'the_sun'})
    self.assertGreater(
        self.TestTracePath.function_measuring_list[0].elapsed_time, 0)
    self.assertEqual(
        self.TestTracePath.function_mapping_list[1].called,
        'newfunc')
    self.assertEqual(
        self.TestTracePath.function_mapping_list[1].caller,
        'setUp')
    self.assertEqual(
        str(self.TestTracePath.function_mapping_list[0].args),
        '(2,)')
    self.assertGreater(
        self.TestTracePath.function_measuring_list[1].elapsed_time, 0)
    # Ensure graph is empty because we haven't explicitly added anything
    self.assertEqual(self.TestTracePath.graph.number_of_nodes(), 0)
    self.assertEqual(self.TestTracePath.graph.number_of_edges(), 0)
    # self.assertEqual(newfunc(2), 2)
    # }}}
# }}}
# {{{ Test Graph Construction


class TestTracePathGraphConstruction(unittest.TestCase):
    # {{{ Graph Setup
    def setUp(self):
        self.EmptyTestTracePath = TracePath(
            name="EmptyTestTracePath", instrument=True)
        self.TestTracePath = TracePath(name="TestTracePath", instrument=True)
        self.TestTracePath.construct_graph()

        @self.TestTracePath.inspect_function_execution
        def myfunc(x, y="the_moon"):
            return x
        # Check if function invocations captured properly
        myfunc(2, y="the_sun")

        @self.TestTracePath.inspect_function_execution
        def newfunc(x):
            return x
        self.EmptyTestTracePath.construct_graph()
        # Instrument another function and check if we don't change behavior
        # and we capture arguments and execution time
        newfunc(2)
        self.empty_graph_file_name = tempfile.mktemp(suffix='.svg')
        self.graph_file_name = tempfile.mktemp(suffix='.svg')
        # }}}
        # {{{ Test Empty Graph Case

    def test_construct_empty_graph(self):
        # Create the empty graph itself, construct_graph creates nodes
        # and edges
        # We called no functions from this function, hence 0 nodes and 0 edges
        self.assertEqual(self.EmptyTestTracePath.graph.number_of_nodes(), 0)
        self.assertEqual(self.EmptyTestTracePath.graph.number_of_edges(), 0)
        self.assertRaises(
            KeyError,
            self.EmptyTestTracePath.graph.get_node,
            'does_not_exist')
        self.assertFalse(os.path.isfile(self.empty_graph_file_name))
        # Write the file
        self.TestTracePath.draw_graph(filename=self.empty_graph_file_name)
        self.assertTrue(os.path.isfile(self.empty_graph_file_name))
        # }}}
# {{{ Test Non Empty Graph Case

    def test_construct_non_empty_graph(self):
        # Create the graph itself, construct_graph creates nodes and edges
        self.TestTracePath.construct_graph()
        # We called two functions from this function, hence 3 nodes and 2 edges
        self.assertEqual(self.TestTracePath.graph.number_of_nodes(), 3)
        self.assertEqual(self.TestTracePath.graph.number_of_edges(), 2)
        self.assertEqual(self.TestTracePath.get_node(
            'newfunc').get_name(), 'newfunc')
        self.assertEqual(self.TestTracePath.get_node(
            'newfunc').get_name(), 'newfunc')
        self.assertFalse(os.path.isfile(self.graph_file_name))
        # Draw the graph itself
        self.TestTracePath.draw_graph(filename=self.graph_file_name)
        self.assertTrue(os.path.isfile(self.graph_file_name))
        # }}}
    # {{{ Graph Tear Down

    def tearDown(self):
        if os.path.isfile(self.graph_file_name):
            os.remove(self.graph_file_name)
        if os.path.isfile(self.empty_graph_file_name):
            os.remove(self.empty_graph_file_name)
            # }}}
# }}}
