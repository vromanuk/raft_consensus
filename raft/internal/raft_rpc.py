import pickle
from dataclasses import dataclass

from raft.internal.raft_entities import LogEntry


@dataclass(frozen=True)
class RaftRpcMessage:
    source_id: int
    dest_id: int
    term: int | None


@dataclass(frozen=True)
class SubmitCommand(RaftRpcMessage):
    command: str


@dataclass(frozen=True)
class AppendEntries(RaftRpcMessage):
    """Sent from leader -> follower to replicate log entries."""

    prev_log_index: int
    prev_log_term: int
    entries: list[LogEntry]
    leader_commit: int


@dataclass(frozen=True)
class AppendEntriesResponse(RaftRpcMessage):
    """Sent from follower -> leader to acknowledge AppendEntries"""

    success: bool
    match_index: int  # Not in paper. In TLA+ spec.


@dataclass(frozen=True)
class RequestVote(RaftRpcMessage):
    candidate_id: int
    last_log_index: int
    last_log_term: int


@dataclass(frozen=True)
class RequestVoteResponse(RaftRpcMessage):
    vote_granted: bool


@dataclass(frozen=True)
class ClockTick(RaftRpcMessage):
    # Sent from outer layer to indicate passage of time.
    seconds: float


def decode_message(data: bytes) -> RaftRpcMessage:
    return pickle.loads(data)


def encode_message(message: RaftRpcMessage) -> bytes:
    return pickle.dumps(message)
