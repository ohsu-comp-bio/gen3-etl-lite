"""Replicate gen3 Program."""
from graph_helper import graph_connect, traverse_node


def copy_node(node):
    """Replicate the node."""
    # TODO - write to ES
    print(node.to_json())


def main():
    """Entrypoint."""
    graph, models = graph_connect()
    with graph.session_scope():
        traverse_node(graph.nodes(models.Program).first(), copy_node)


if __name__ == "__main__":
    main()
