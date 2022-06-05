# {{{ Imports
import inspect
from functools import wraps
import time
import pygraphviz as pgv
import collections
# }}}
# {{{ TracePath class


class TracePath:
    # {{{ __init__ function
    def __init__(self, instrument=True):
        self.instrument = instrument
        # self.counter = 0
        # self.counter_dictionary = {}
        self.function_mapping_list = []
        self.function_measuring_list = []
        self.graph = pgv.AGraph(directed=True, strict=True)
        # self.graph.attr["color"] = "white"
        # self.execution_counter = 0
        # }}}
        # {{{ Inspect function

    def inspect_function_execution(self, func):
        @wraps(func)
        def _(*args, **kwargs):
            if self.instrument:
                caller = inspect.stack()[1].function
                called = func.__name__
                print(f"Caller: {caller}, Called: {called}")
                function_mapping = collections.namedtuple(
                    "function_mapping", "caller called")
                function_measurement = collections.namedtuple(
                    "function_measurement", "caller called elapsed_time"
                    )
                local_mapping_tuple = function_mapping(caller, called)
                # timing_counter = self.counter
                self.function_mapping_list.append(local_mapping_tuple)
                print(
                    f"Executing function named: {func.__name__}, with arguments: {args}, and keyword arguments: {kwargs}."
                    )
                print(f"Executing function named: {func.__name__}")
                # print(f"From wrapper function: {func}")
                start_time = time.time()
                return_value = func(*args, **kwargs)
                end_time = time.time()
                elapsed_time = end_time - start_time
                local_measurement_tuple = function_measurement(
                    caller, called, elapsed_time
                    )
                self.function_measuring_list.append(local_measurement_tuple)
                # self.counter_dictionary[self.counter].append(local_mapping_tuple)
                print(
                    f"From wrapper function: Execution of {func.__name__} took {elapsed_time} seconds."
                    )

                return return_value
            else:
                return func(*args, **kwargs)  # Case where no instrumentation

        return _
        # }}}
    # {{{ get_node

    def get_node(self, node_label):
        try:
            node = self.graph.get_node(self.graph, node_label)
        except BaseException:
            KeyError
            self.graph.add_node(node_label)
            node = self.graph.get_node(node_label)
        return node

    # }}}
    # {{{ add_edges(tuple_list, self.graph):

    def add_edges(self):
        stack = collections.deque()
        sequence = 0
        bad_nodes = []
        for function_map in self.function_mapping_list:
            caller_label = function_map.caller
            called_label = function_map.called
            source_node = self.get_node(caller_label)
            destination_node = self.get_node(called_label)
            if len(stack) == 0:
                self.graph.add_edge(
                    source_node, destination_node, label=sequence)
                stack.append(caller_label)
            else:
                if caller_label in stack:
                    # Pop the stack off till the last node that called this one
                    while stack[-1] != caller_label:
                        stack.pop()
                elif caller_label not in stack:
                    print(
                        f"caller {caller_label} and called {called_label} have issues. {caller_label} called when it was not on the stack."
                        )
                    bad_nodes.append(caller_label)
                    bad_node = self.get_node(caller_label)
                    bad_node.attr["color"] = "orange"
                    continue
                self.graph.add_edge(
                    source_node, destination_node, label=sequence)
            stack.append(called_label)
            sequence += 1
        for node in bad_nodes:
            bad_node = self.get_node(node)
            bad_node.attr["color"] = "orange"

    #        # }}}
    # {{{ Draw graph

    def draw_graph(self, filename, *args, **kwargs):
        self.graph.layout()
        self.graph.layout(prog="dot")
        self.graph.draw(filename)
        # pgv.AGraph.close(filename)
        # tempgraph = graph
        # def self.graph_writer(tempgraph, filename):
        #     tempgraph.draw(filename)
        #     tempgraph.close(filename)
        # graph_writer(tempgraph, filename)
        # }}}
