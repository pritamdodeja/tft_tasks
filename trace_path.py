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
    def __init__(self, instrument=True, name=None):
        self.name = name
        self.instrument = instrument
        self.function_mapping_list = []
        self.function_measuring_list = []
        self.graph = pgv.AGraph(directed=True, strict=False, rankdir="LR",
                                outputorder="labelsfirst")
        # }}}
    # {{{ __del__ function

    def __del__(self, instrument=True, name=None):
        # print(f"Destructor called on {self.name}")
        del(self.name)
        del(self.instrument)
        del(self.function_mapping_list)
        del(self.function_measuring_list)
        del(self.graph)
        del(self)
        return True
        # }}}
        # {{{ Inspect function

    def inspect_function_execution(self, func):
        @wraps(func)
        def _(*args, **kwargs):
            if self.instrument:
                caller = inspect.stack()[1].function
                called = func.__name__
                function_mapping = collections.namedtuple(
                    "function_mapping", "caller called args kwargs")
                function_measurement = collections.namedtuple(
                    "function_measurement", "caller called elapsed_time"
                    )
                local_mapping_tuple = function_mapping(caller, called, args,
                                                       kwargs)
                self.function_mapping_list.append(local_mapping_tuple)
                start_time = time.time()
                return_value = func(*args, **kwargs)
                end_time = time.time()
                elapsed_time = end_time - start_time
                local_measurement_tuple = function_measurement(
                    caller, called, elapsed_time
                    )
                self.function_measuring_list.append(local_measurement_tuple)

                return return_value
            else:
                return func(*args, **kwargs)  # Case where no instrumentation

        return _
        # }}}
    # {{{ get_node

    def get_node(self, node_label):
        try:
            node = self.graph.get_node(node_label)
        except KeyError as e:
            raise(e)
        return node

    def add_node(self, node_label):
        try:
            self.graph.get_node(node_label)
        except KeyError:
            self.graph.add_node(node_label)

    # }}}
    # {{{ construct_graph(tuple_list, self.graph):

    def construct_graph(self):
        stack = collections.deque()
        sequence = 0
        bad_nodes = []
        for function_map in self.function_mapping_list:
            caller_label = function_map.caller
            called_label = function_map.called
            # arguments = str(function_map.args) + str(function_map.kwargs)
            try:
                source_node = self.get_node(caller_label)
            except KeyError:
                self.add_node(caller_label)
                source_node = self.get_node(caller_label)

            try:
                destination_node = self.get_node(called_label)
            except KeyError:
                self.add_node(called_label)
                destination_node = self.get_node(called_label)

            if len(stack) == 0:
                self.graph.add_edge(
                    source_node, destination_node, label=sequence)
                stack.append(caller_label)
            elif len(stack) > 0:
                if caller_label in stack:
                    # Pop the stack off till the last node that called this one
                    while stack[-1] != caller_label:
                        stack.pop()
                elif caller_label not in stack:
                    print(
                        f"caller {caller_label} and called {called_label} have"
                        f"issues. {caller_label} called when it was not on the"
                        f"stack."
                        )
                    bad_nodes.append(caller_label)
                    bad_node = self.get_node(caller_label)
                    # continue
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
        # }}}
