from enum import Enum


class Runner(Enum):
    ASYNC = "async"
    THREAD = "thread"
    PROCESS = "process"
    SYNC = "sync"
