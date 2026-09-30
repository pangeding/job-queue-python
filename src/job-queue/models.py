from enum import Enum

class TaskStatus(str, Enum):
    PENDING   = "pending"
    QUEUED    = "queued"
    RUNNING   = "running"
    SUCCESS   = "success"
    FAILURE   = "failure"
    RETRY     = "retry"
    CANCELLED = "cancelled"
