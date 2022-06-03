
from ipdb import set_trace as ipdb
from IPython import embed as ipython

def return_none():
    return None

def make_tree(list_of_mappings):
    try:
        return_tree
    except NameError:
        return_tree = collections.defaultdict(return_none)
    current_node = list_of_mappings[0]
    current_node_name = current_node.caller
    current_node_child = current_node.called
    return_tree['name'] = current_node_name
    if not current_node_child: #There isn't a child
        current_node_child = None
        return_tree['children'] = [current_node_child]
        # return return_tree
    else: #There is a child
        return_tree['children'] = [make_tree([function_mapping(current_node_child,"")])]
    if len(list_of_mappings) > 1:
        rest_of_nodes = list_of_mappings[1:]
        if rest_of_nodes[0].caller == current_node_name:
            remaining_tree = make_tree([function_mapping(rest_of_nodes[0].called, "")])
            return_tree["children"].append(remaining_tree)
        elif rest_of_nodes[0].caller == current_node_child:
            remaining_tree = make_tree([function_mapping(rest_of_nodes[0].caller, rest_of_nodes[0].called)])
            # Two scenarios, children present or not
            if return_tree["children"][-1]["children"] == [None]:
                return_tree["children"][-1]["children"] = remaining_tree
            else:
                return_tree["children"][-1]["children"].append(remaining_tree)

    return return_tree

def make_tree(list_of_mappings):
    try:
        return_tree
    except NameError:
        return_tree = collections.defaultdict(return_none)
    try:
        child_tree
    except NameError:
        child_tree = collections.defaultdict(return_none)
    if len(list_of_mappings) == 0:
        return return_tree
    current_node = list_of_mappings[0]
    current_node_name = current_node.caller
    current_node_child = current_node.called
    return_tree["name"] = current_node_name
    print(f"Beginning Processing node {current_node} with caller {current_node_name} and child {current_node_child}")
    if not current_node_child:
        current_node_child = None
        return_tree['children'] = [current_node_child]
    else:
        return_tree['children'] = [make_tree([function_mapping(current_node_child, "")])]
    if len(list_of_mappings) > 1:
        child_tree = make_tree(list_of_mappings[1:])
        # Is root of child tree the same as return tree?
        root_of_child_tree = child_tree["name"]
        print(f"Child tree's root is {root_of_child_tree}")
        print(f"Child tree is {child_tree}")
        names_of_children = [child["name"] for child in return_tree["children"]]
        print(f"children's names are {names_of_children}")
        if return_tree["name"] == root_of_child_tree:
            print(f"Going to attach to base of return tree")
            return_tree["children"].append(child_tree["children"])
            return child_tree
        elif root_of_child_tree in names_of_children:
            print(f"Going to attach to one of the children")
            for index, child in enumerate(return_tree["children"]):
                print(f'Child is {child} and child type is {type(child)} and name is {child["name"]}')
                child_tree_name = child["name"]
                if root_of_child_tree == child_tree_name:
                    index_to_attach = index
                    print(f"Index to attach is {index_to_attach}")
            return_tree["children"][index_to_attach]["children"].append(child_tree["children"])
            return child_tree
                    # return_tree["children"].append(child_tree["children"])
        else:
            print("Unhandled scenario, need to attach higher up in the tree")
            print(child_tree)
            print(f'Return tree name is {return_tree["name"]}')
            return child_tree


    return return_tree

tuple_list = [function_mapping("a", "b"), function_mapping("b", "c"), function_mapping("a", "c")]
tuple_list = [function_mapping("a", "")]
make_tree(tuple_list)
tuple_list = [function_mapping("a", "b")]
output = make_tree(tuple_list)
tuple_list = [function_mapping("main", "b"), function_mapping("b", "c")]
output = make_tree(tuple_list)
tuple_list = [function_mapping("main", "b"), function_mapping("main", "c")]
output = make_tree(tuple_list)
tuple_list = [function_mapping("main", "b"), 
              function_mapping("b", "c"),
              function_mapping("main", "e"),
              function_mapping("e", "b"),
              function_mapping("main", "c"),
              ]
output = make_tree(tuple_list)


from collections import namedtuple
import collections
function_mapping = collections.namedtuple("function_mapping",
"caller called")

simplest_mapping = function_mapping("main", "")
simplest_list_of_mappings = [simplest_mapping]
make_tree(simplest_list_of_mappings)

next_simplest_mapping = function_mapping("main", "function_a")
next_simplest_list_of_mappings = [next_simplest_mapping]
make_tree(next_simplest_list_of_mappings)

a1 = function_mapping("main", "function_a")
a2 = function_mapping("main", "function_b")
mapping_1 = [a1, a2]
make_tree(mapping_1)

output = make_tree([function_mapping("main", "a")])
output = make_tree([function_mapping("main", "a"), 
           function_mapping("main", "b"),
           ])
output = make_tree([function_mapping("main", "a"), 
           function_mapping("main", "b"),
           function_mapping("b", "c"),
           ])
output = make_tree([function_mapping("main", "a"), 
           function_mapping("main", "b"),
           function_mapping("b", "c"),
           function_mapping("c", "d"),
           function_mapping("d", "e"),
           function_mapping("e", "b"),
           ])
make_tree([function_mapping("main", "a"), 
           function_mapping("main", "b"),
           function_mapping("b", "a"),
           ])
make_tree([function_mapping("main", "a"), function_mapping("main", "b")])
make_tree([function_mapping("a", "b"), 
           function_mapping("a", "c"),
           function_mapping("c", "d"),
           ])

def find_node(tree, node_label):
    found = False
    if tree["name"] == node_label:
        found = True
    else:
        if tree["children"]:
            for child_tree in tree["children"]:
                print(f"working on child tree {child_tree}")
                if isinstance(child_tree, list):
                    for child in child_tree:
                        found = find_node(child, node_label)
                else:
                    found = find_node(child_tree, node_label)
                if found:
                    break
    return found
def attach_subtree(full_tree, subtree):
    # ipdb()
    print(f"Breaking at {full_tree['name']}")
    if full_tree["name"] == subtree["name"]:
        full_tree["children"].append(subtree["children"][0])
    else:
        for child_of_full_tree in full_tree["children"]:
            partial_tree = attach_subtree(child_of_full_tree, subtree)
            full_tree["children"].append(partial_tree["children"])
    return full_tree


def make_tree(list_of_mappings):
    try:
        return_tree
    except NameError:
        return_tree = collections.defaultdict(return_none)
    try:
        child_tree
    except NameError:
        child_tree = collections.defaultdict(return_none)
    if len(list_of_mappings) == 0:
        return return_tree
    for position, node in enumerate(list_of_mappings):    
        current_node = node
        current_node_name = current_node.caller
        current_node_child = current_node.called
        print(f"Beginning Processing node {current_node} with caller {current_node_name} and child {current_node_child} at position {position}")
        node_exists = find_node(return_tree, current_node_name)
        if node_exists:
            print(f"Found node at position {position}")
            subtree = make_tree([node])
            if return_tree["name"] == node.caller:
                print(f"found it at level 0")
                return_tree = attach_subtree(return_tree, subtree)
            else:
                print(f"Did not find it.")
                # ipdb()
                for position, child in enumerate(return_tree["children"]):
                    if child["name"] == node.caller:
                        pass
                        return_tree["children"][position]["children"].append(subtree["children"][0])
                        # ipdb()
                        # partial_tree = attach_subtree(child, subtree)
                        # return_tree = attach_subtree(return_tree, partial_tree)
                # return_tree = attach_subtree(return_tree, subtree)
            print(f"subtree is going to be {subtree}")
            #Attach to node
        else:
            #Create new node and attach
            return_tree["name"] = current_node_name
            return_tree['children'] = [{"name": current_node_child, "children": []}]
        print(f"The value of node_exists for node named {current_node_name} is {node_exists}")
    return return_tree


def parse_tree(tree):
    # ipdb()
    print(f"Name of node is {tree['name']}")
    if len(tree["children"]) > 0:
        for child in tree["children"]:
            parse_tree(child)

def generate_sequence_list(tree):
    # ipdb()
    print(f"sequence of node is {tree['sequence']}")
    if len(tree["children"]) > 0:
        for child in tree["children"]:
            generate_sequence_list(child)

def count_depth(tree):
    try:
        depth
    except NameError:
        depth = 0
    print("hi")
    if len(tree["children"]) > 0:
        depth = depth + 1
        for child_tree in tree["children"]:
            depth = depth + count_depth(child_tree)
    return depth

play_tree = {
            "name": "root", 
    "children": [ {"name": "a", "children": [ {"name": "c", "children" : []      }     ]    }, { "name": "b", "children": [ {"name": "d", "children": [ ]     }   ]    }

             ] 
             }
import pydot
new_play_tree = {'name': 'root',
 'children': [{'name': 'a', 'children': [{'name': 'c', 'children': []}]},
  {'name': 'b',
   'children': [{'name': 'd',
     'children': [{'name': 'subroot',
       'children': [{'name': 'g', 'children': [{'name': 'h', 'children': []}]},
        {'name': 'i', 'children': [{'name': 'j', 'children': []}]}]}]}]}]}

graph = pydot.Dot("my_graph", graph_type="digraph", bgcolor="blue")
execution_counter = 0
def parser_nested_dictionary(dictionary, parent_node_label=None):
    global execution_counter
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
                label=execution_counter)
                execution_counter += 1
                graph.add_edge(my_edge)
                print(f"Going to make node {parent_node} the parent of{current_node_label}")
            else:
                graph.add_node(current_node)

        elif key == 'children':
            for child in value:
                print(f"Recursing with current_node_label as {current_node_label}")
                parser_nested_dictionary(child, parent_node_label=current_node_label)
parser_nested_dictionary(new_play_tree)
graph.write_jpeg("test_graph.jpeg")

subtree = {'name': 'c', 'children': [{'name': 'm', 'children': []  }]}
def add_subtree(tree, subtree):
    # ipdb()
    print(f"root of subtree is {subtree['name']}")
    # ipdb()
    if tree["name"] == subtree["name"]:
        # ipdb()
        tree["children"].append(subtree["children"][0])
        # tree["sequence"] = subtree["sequence"]
        return tree
    else:
        for child_tree in tree["children"]:
            add_subtree(child_tree, subtree)

def new_add_subtree(tree, subtree):
    # ipdb()
    print(f"root of subtree is {subtree['name'][0]}")
    # ipdb()
    if tree["name"][0] == subtree["name"][0]:
        # ipdb()
        tree["children"].append(subtree["children"][0])
        # tree["sequence"] = subtree["sequence"]
        return tree
    else:
        for child_tree in tree["children"]:
            new_add_subtree(child_tree, subtree)
subtree = {'name': 'root', 'children': [{'name': 'm', 'children': []}]}    
modified_tree = add_subtree(new_play_tree, subtree)

graph = pydot.Dot("my_graph", graph_type="digraph", bgcolor="blue")
execution_counter = 0
parser_nested_dictionary(modified_tree)
graph.write_jpeg("modified_tree.jpeg")



subtree = {'name': 'i', 'children': [{'name': 'n', 'children': []}]}    
new_modified_tree = add_subtree(new_play_tree, subtree)
graph = pydot.Dot("my_graph", graph_type="digraph", bgcolor="blue")
execution_counter = 0
parser_nested_dictionary(new_play_tree)
graph.write_jpeg("new_modified_tree.jpeg")

function_mapping = collections.namedtuple("function_mapping",
"caller called sequence")
tuple_list = [function_mapping("main", "b", 0), 
              function_mapping("b", "c", 1),
              function_mapping("main", "e", 2),
              function_mapping("e", "b", 3),
              function_mapping("main", "c", 4),
              ]
tree = {"name": "root", "children": []}
subtree = {"name": "root", "children": [{"name": "a", "children": []}]}
add_subtree(tree, subtree)
graph = pydot.Dot("my_graph", graph_type="digraph", bgcolor="blue")
execution_counter = 0
parser_nested_dictionary(tree)
graph.write_jpeg("new_tree.jpeg")

def make_tree(tuple_list):
    # sequence_list = []
    try:
        return_tree
    except NameError:
        return_tree = {}
    try:
        subtree
    except NameError:
        subtree = {}
    for count, function_map in enumerate(tuple_list):
        try:
            return_tree["name"]
        except KeyError:
            return_tree["name"] = function_map.caller
            # return_tree["sequence"] = function_map.sequence
            print(f"sequence number is {function_map.sequence}")
            return_tree["children"] = []
        print(f"count is {count} and function map is {function_map}")
        # sequence_list.append(function_map.sequence)
        subtree["name"] = function_map.caller
        subtree["children"] = [{"name": function_map.called, "children":[]}]
        # subtree["sequence"] = function_map.sequence
        add_subtree(return_tree, subtree)
    return return_tree

tuple_list = [function_mapping("main", "b", 0), 
              function_mapping("b", "c", 1),
              function_mapping("main", "e", 2),
              function_mapping("e", "b", 3),
              function_mapping("main", "c", 4),
              function_mapping("c", "g", 5),
              function_mapping("c", "b", 6),
              function_mapping("c", "d", 7),
              function_mapping("d", "b", 8),
              function_mapping("d", "e", 9),
              function_mapping("c", "h", 10),
              function_mapping("main", "i", 11),
              function_mapping("i", "j", 12),
              function_mapping("main", "k", 13),
              function_mapping("main", "l", 14),
              function_mapping("l", "m", 15),
              function_mapping("l", "n", 16),
              ]
output = make_tree(tuple_list)
new_output = new_make_tree(tuple_list)
graph = pydot.Dot("my_graph", graph_type="digraph", bgcolor="blue")
add_edge(tuple_list)
new_parser_nested_dictionary(new_output)
graph.write_jpeg("new_tree_without_labels.jpeg")
def new_parser_nested_dictionary(dictionary, parent_node_label=None):
    sequence_position = 0
    for key, value in dictionary.items():
        # ipdb()
        if key == 'sequence':
            execution_counter_number = value
        if key == 'name':
            current_node_label = value[0]
            current_node_sequence = value[1]
            if not graph.get_node(current_node_label):
                current_node = pydot.Node(current_node_label,
            label=current_node_label)
                graph.add_node(current_node)
            else:
                current_node = graph.get_node(current_node_label)

                # current_node = pydot.Node(current_node_label,
                # label=current_node_label)
            print(f"current node label is {current_node_label}")
            if parent_node_label:
                print(f"parent node label is {parent_node_label}")
                if not graph.get_node(parent_node_label):
                    parent_node = pydot.Node(parent_node_label,
                label=parent_node_label)
                else:
                    parent_node = graph.get_node(parent_node_label)
                # my_edge = pydot.Edge(src=parent_node, dst=current_node,
                # label=current_node_sequence)
                sequence_position += 1
                # execution_counter += 1
                # graph.add_edge(my_edge)
                # graph.add_node(parent_node)
                # graph.add_node(current_node)
                print(f"Going to make node {parent_node} the parent of{current_node_label}")
            else:
                graph.add_node(current_node)

        elif key == 'children':
            for child in value:
                print(f"Recursing with current_node_label as {current_node_label}")
                new_parser_nested_dictionary(child, parent_node_label=current_node_label)

def new_make_tree(tuple_list):
    # sequence_list = []
    try:
        return_tree
    except NameError:
        return_tree = {}
    try:
        subtree
    except NameError:
        subtree = {}
    for count, function_map in enumerate(tuple_list):
        try:
            return_tree["name"]
        except KeyError:
            return_tree["name"] = (function_map.caller, function_map.sequence)
            # return_tree["sequence"] = function_map.sequence
            print(f"sequence number is {function_map.sequence}")
            return_tree["children"] = []
        print(f"count is {count} and function map is {function_map}")
        # sequence_list.append(function_map.sequence)
        subtree["name"] = (function_map.caller, function_map.sequence)
        subtree["children"] = [{"name": (function_map.called, function_map.sequence), "children":[]}]
        # subtree["sequence"] = function_map.sequence
        new_add_subtree(return_tree, subtree)
    return return_tree

def add_edges(tuple_list):
    for function_map in tuple_list:
        caller_label = function_map.caller
        called_label = function_map.called
        sequence = function_map.sequence
        source_node = graph.get_node(caller_label)[0]
        destination_node = graph.get_node(called_label)[0]
        print(f"type of source_node is {type(source_node)}")
        current_edge = pydot.Edge(src=source_node, dst=destination_node, label=sequence)
        graph.add_edge(current_edge)
