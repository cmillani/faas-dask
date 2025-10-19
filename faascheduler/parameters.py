from enum import Enum, auto


class Command(Enum):
    exec = auto()
    store = auto()
    graph = auto()
    graph_node = auto()
    echo = auto()


class SchedulerStrategy(Enum):
    local = auto()
    remote = auto()
