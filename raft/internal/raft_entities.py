from dataclasses import dataclass
from enum import Enum


@dataclass(frozen=True)
class LogEntry:
    term: int
    command: str


class RaftCommand(Enum):
    START_LEADER_ELECTION = 0
    STEP_DOWN_TO_FOLLOWER = 1
    BECOME_LEADER = 2
    ELECTION_TIMEOUT_EXPIRED = 3


@dataclass(frozen=True)
class ClusterInfo:
    cluster_size: int
    topology: dict[int, tuple[str, int]]
