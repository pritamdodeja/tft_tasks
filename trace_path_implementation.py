# import inspect
# from functools import wraps
# import pydot
from trace_path import TracePath
MyTracePath = TracePath(instrument=True)
@MyTracePath.inspect_function_execution
def function_a(a, b, c=3):
    print(f"From function function_a.")
    kangaroo = "kangaroo"
    function_b()
    # get_stack()
    # print("From function a")
    # function_b()


@MyTracePath.inspect_function_execution
def function_b():
    print(f"From function function_b.")
    # print("Monkeys")
    pass

@MyTracePath.inspect_function_execution
def function_c():
    print(f"From function function_c.")
    function_d()

@MyTracePath.inspect_function_execution
def function_e():
    print(f"From function function_e.")
    function_b()

@MyTracePath.inspect_function_execution
def function_d():
    print(f"From function function_d.")
    function_a(3, 4, c=8)

@MyTracePath.inspect_function_execution
def main():
    kangaroo = "kangaroo"
    # get_stack()
    # function_a(1, 2, c=4)
    function_b()
    function_e()
    function_c()
    function_d()
    function_c()
    MyTracePath.parser_nested_dictionary(dictionary=MyTracePath.dict_writer())
    MyTracePath.graph.write_jpeg('./new_oop_graph.jpg')



if __name__ == '__main__':
    main()
