# {{{ Imports
import unittest
from trace_path import TracePath
import pygraphviz as pgv
import os
# }}}
# {{{ Test Constructor
class TestTracePathConstructor(unittest.TestCase):
    def setUp(self):
        self.TestTracePath = TracePath(name="TestTracePath")
        self.TestTracePathInstrumentationDisabled = TracePath(name="TestTracePathInstrumentationDisabled", instrument=False)
    def test_constructor(self):
        self.assertTrue(isinstance(self.TestTracePath, TracePath))
        self.assertTrue(self.TestTracePath.instrument, True)
        self.assertEqual(self.TestTracePathInstrumentationDisabled.instrument, False)
        self.assertTrue(isinstance(self.TestTracePath.function_mapping_list, list))
        self.assertTrue(isinstance(self.TestTracePath.function_measuring_list, list))
        self.assertTrue(isinstance(self.TestTracePath.graph, pgv.AGraph))

# }}}
# {{{ Test Instrumentation Decorator
class TestTracePathConstructor(unittest.TestCase):
    def setUp(self):
        self.TestTracePath = TracePath(name="TestTracePath", instrument=True)

    def test_inspect_function_execution(self):
        self.assertEqual(len(self.TestTracePath.function_mapping_list),0)
        self.assertEqual(self.TestTracePath.graph.number_of_nodes(),0)
        @self.TestTracePath.inspect_function_execution
        def myfunc(x, y="the_moon"):
            return x
        self.assertEqual(myfunc(2, y="the_sun"), 2)
        
        self.assertEqual(len(self.TestTracePath.function_mapping_list),1)
        self.assertEqual(len(self.TestTracePath.function_measuring_list),1)
        self.assertEqual(self.TestTracePath.function_mapping_list[0].called, 'myfunc')
        self.assertEqual(self.TestTracePath.function_mapping_list[0].caller, 'test_inspect_function_execution')
        self.assertEqual(str(self.TestTracePath.function_mapping_list[0].args), '(2,)')
        self.assertEqual(self.TestTracePath.function_mapping_list[0].kwargs, {'y': 'the_sun'})
        self.assertGreater(self.TestTracePath.function_measuring_list[0].elapsed_time,0)
        self.assertEqual(self.TestTracePath.graph.number_of_nodes(),0)
        self.assertEqual(self.TestTracePath.graph.number_of_edges(),0)
        # self.TestTracePath.add_edges()
        # self.assertEqual(self.TestTracePath.graph.number_of_nodes(),2)
        # self.assertEqual(self.TestTracePath.graph.number_of_edges(),1)
        # self.assertFalse(os.path.isfile("test.svg"))
        # self.TestTracePath.draw_graph(filename="test.svg")
        # self.assertTrue(os.path.isfile("test.svg"))
        @self.TestTracePath.inspect_function_execution
        def newfunc(x):
            return x
        self.assertEqual(newfunc(2), 2)
        self.assertEqual(len(self.TestTracePath.function_mapping_list),2)
        self.assertEqual(len(self.TestTracePath.function_measuring_list),2)
        self.assertEqual(self.TestTracePath.function_mapping_list[1].called, 'newfunc')
        self.assertEqual(self.TestTracePath.function_mapping_list[1].caller, 'test_inspect_function_execution')
        self.assertEqual(str(self.TestTracePath.function_mapping_list[0].args), '(2,)')
        # self.assertEqual(self.TestTracePath.function_mapping_list[0].kwargs, {'y': 'the_sun'})
        self.assertGreater(self.TestTracePath.function_measuring_list[1].elapsed_time,0)
        # self.assertEqual(self.TestTracePath.graph.number_of_nodes(),2)
        # self.assertEqual(self.TestTracePath.graph.number_of_edges(),1)
        self.TestTracePath.add_edges()
        self.assertEqual(self.TestTracePath.graph.number_of_nodes(),3)
        self.assertEqual(self.TestTracePath.graph.number_of_edges(),2)
        self.assertEqual(self.TestTracePath.get_node('newfunc').get_name(),'newfunc')
        self.assertFalse(os.path.isfile("test.svg"))
        self.TestTracePath.draw_graph(filename="test.svg")
        self.assertTrue(os.path.isfile("test.svg"))
    
    def tearDown(self):
        pass
        if os.path.isfile("test.svg"):
            os.remove("test.svg")

# }}}
