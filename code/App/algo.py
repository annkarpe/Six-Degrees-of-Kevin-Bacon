
from queue import Queue

kevin = "Kevin Bacon"
graph = {}

def read_graph_from_txt(filename):
    try:
        file = open(filename)
    except FileNotFoundError:
        print('File', filename, 'is not found!')
    else:
        with file:
            for line in file:
                actor, *coactors = map(str.strip, line.split(','))
                graph[actor] = coactors


def find_degree(actor):
    if actor == kevin:
        return 0

    q_actor_with_degree = Queue()
    visited_actors = set()

    q_actor_with_degree.put((kevin, 0))

    while not q_actor_with_degree.empty():
        current_actor, current_degree = q_actor_with_degree.get()

        for actor_connected_to_current in graph.get(current_actor, []):
            if actor_connected_to_current == actor:
                return current_degree + 1

            if actor_connected_to_current not in visited_actors:
                visited_actors.add(actor_connected_to_current)
                q_actor_with_degree.put((actor_connected_to_current, current_degree + 1))

    return -1

