import inspect
from functools import wraps
import pydot
class TracePath:
    def __init__(self, instrument=True):
        self.instrument = instrument
        self.counter = 0
        self.counter_dictionary = {}
        self.counter_dictionary[0] = []
        self.graph = pydot.Dot("my_graph", graph_type="digraph", bgcolor="blue")
        self.execution_counter = 0
    def inspect_function_execution(self, func):
        @wraps(func)
        def _(*args, **kwargs):
            if self.instrument:
                caller = inspect.stack()[1].function
                called = func.__name__
                print(f"Caller: {caller}, Called: {called}")
                if caller == 'main':
                    self.counter = self.counter + 1
                    self.counter_dictionary[self.counter] = []

                    # execution_tree[caller] = called
                    # execution_tree[caller] = dictionary_writer(caller, called)
                    print("Main case")
                else:
                # if not caller.find('line'):
                    if 'cell line' in caller:
                        print("Found cell line")
                    else:
                        print("Did not find it")
                        print(f"Non-main case {caller}")
                self.counter_dictionary[self.counter].append((caller, called))
                        # throwaway_dict = collections.OrderedDict()
                        # throwaway_dict[caller] = called
                        # # execution_tree[caller] = throwaway_dict
                        # execution_tree[caller] = dictionary_writer(throwaway_dict, called)
                        # Create new dictionary
                # print(f"Type of caller is: {type(caller)}")
                # print(f"Was called from: {inspect.stack()[1].function}.")
                # print(f"Executing function named: {func.__name__}, with arguments: {args}, and keyword arguments: {kwargs}.")
                # print(f"Executing function named: {func.__name__}")
                # print(f"From wrapper function: {func}")
                # start_time = time.time()
                return_value = func(*args, **kwargs)
                # end_time = time.time()
                # elapsed_time = end_time - start_time
                # print(f"From wrapper function: Execution of {func.__name__} took {elapsed_time} seconds.")
                
                return return_value
            else:
                return func(*args, **kwargs)
        return _

    def new_tuple_writer(self, tuple_list):
        return_dictionary = {}
        current_dictionary = {}
        for count in range(len(tuple_list) -1, -1, -1):
            # print(tuple_list[count])
            if current_dictionary == {}:
                current_dictionary = {'name': tuple_list[count][1], 'children': []}
                current_dictionary = {'name': tuple_list[count][0], 'children':[current_dictionary]}
                
            else:
                current_dictionary = {'name': tuple_list[count][0], 'children':
                [current_dictionary]}
            # return_dictionary = {'name': tuple_list[count][0],  'children': [current_dictionary]}
        return current_dictionary

    def dict_writer(self):
        return_dictionary = {}
        return_list = []
        current_dictionary = {}
        for count in self.counter_dictionary.keys():
            # print(count)
            # print(counter_dictionary[count])
            # print(tuple_writer(counter_dictionary[count]))
            current_dictionary = self.new_tuple_writer(self.counter_dictionary[count])
            # print(current_dictionary['name'] = f"main{count}")
            if current_dictionary['name'] == 'main':
                # print("In here!")
                current_dictionary['name'] = f"main{count}" 
            print(current_dictionary)
            return_list.append(current_dictionary)
        return_dictionary = {'name': 'root', 'children': return_list}
        return return_dictionary
    
    def parser_nested_dictionary(self, dictionary, parent_node_label=None):
        for key, value in dictionary.items():
            if key == 'name':
                current_node_label = value
                current_node = pydot.Node(current_node_label,
                label=current_node_label)
                print(f"current node label is {current_node_label}")
                if parent_node_label:
                    print(f"parent node label is {parent_node_label}")
                    parent_node = pydot.Node(parent_node_label,
                    label=parent_node_label)
                    my_edge = pydot.Edge(src=parent_node, dst=current_node,
                    label=self.execution_counter)
                    self.execution_counter += 1
                    self.graph.add_edge(my_edge)
                    print(f"Going to make node {parent_node} the parent of{current_node_label}")
                else:
                    self.graph.add_node(current_node)

            elif key == 'children':
                for child in value:
                    print(f"Recursing with current_node_label as {current_node_label}")
                    self.parser_nested_dictionary(child, parent_node_label=current_node_label)
# MyTracePath = TracePath()
# @MyTracePath.inspect_function_execution
# def function_a(a, b, c=3):
#     print(f"From function function_a.")
#     kangaroo = "kangaroo"
#     function_b()
#     # get_stack()
#     # print("From function a")
#     # function_b()


# @MyTracePath.inspect_function_execution
# def function_b():
#     print(f"From function function_b.")
#     # print("Monkeys")
#     pass

# @MyTracePath.inspect_function_execution
# def function_c():
#     print(f"From function function_c.")
#     function_d()

# @MyTracePath.inspect_function_execution
# def function_e():
#     print(f"From function function_e.")
#     function_b()

# @MyTracePath.inspect_function_execution
# def function_d():
#     print(f"From function function_d.")
#     function_a(3, 4, c=8)

# @MyTracePath.inspect_function_execution
# def main():
#     kangaroo = "kangaroo"
#     # get_stack()
#     # function_a(1, 2, c=4)
#     function_b()
#     function_e()
#     function_c()
#     function_d()
#     function_c()
#     MyTracePath.parser_nested_dictionary(dictionary=MyTracePath.dict_writer())
#     MyTracePath.graph.write_jpeg('./my_oop_graph.jpg')



# if __name__ == '__main__':
#     main()
