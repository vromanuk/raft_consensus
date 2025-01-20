import logging
import random
import statistics
from abc import ABC, abstractmethod
from enum import Enum
from functools import wraps
from queue import Queue
from typing import Self

from raft.internal.raft_configuration import (
    ELECTION_RANDOM,
    ELECTION_TIMEOUT_SECONDS,
    RAFT_SERVERS,
)
from raft.internal.raft_entities import ClusterInfo, LogEntry, RaftCommand
from raft.internal.raft_replication_log import PreviousLogInfo, RaftReplicationLog
from raft.internal.raft_rpc import (
    AppendEntries,
    AppendEntriesResponse,
    ClockTick,
    RaftRpcMessage,
    RequestVote,
    RequestVoteResponse,
    SubmitCommand,
)


class InvalidStateTransitionError(RuntimeError):
    """Exception raised for invalid state transitions in the Raft state machine."""

    def __init__(self, message: str = "Invalid state transition"):
        self.message = message
        super().__init__(self.message)


class ResultOnTermConflict(Enum):
    NO_OP = 0
    DROP_MESSAGE = 1
    STEP_DOWN = 2


class StateRole(Enum):
    LEADER = 0
    FOLLOWER = 1
    CANDIDATE = 2


def enforce_invariants(method):
    @wraps(method)
    def wrapper(self: "RaftState", message: RaftRpcMessage):
        # Capture the initial state
        orig_commit_index = self.commit_index
        orig_current_term = self.current_term
        orig_role = self.role
        last_committed_entry = self.replication_log.get_entries(
            orig_commit_index, orig_commit_index
        )

        result = method(self, message)

        assert self.commit_index >= orig_commit_index, (
            "commit_index must increase monotonically"
        )
        assert self.current_term >= orig_current_term, (
            "current_term must increase monotonically"
        )
        assert last_committed_entry == self.replication_log.get_entries(
            orig_commit_index, orig_commit_index
        )

        # Prevent bad state transitions. See Figure 4.
        assert not (orig_role == StateRole.FOLLOWER and self.role == StateRole.LEADER)
        assert not (orig_role == StateRole.LEADER and self.role == StateRole.CANDIDATE)

        return result

    return wrapper


class RaftState(ABC):
    def __init__(
        self,
        node_id: int,
        command_queue: Queue,
        current_term: int = 0,
        voted_for: int | None = None,
        replication_log: RaftReplicationLog = None,
        next_index: dict[int, int] = None,
        match_index: dict[int, int] = None,
        election_timeout: int = 0,
        cluster_info: ClusterInfo | None = None,
        votes_received: set[int] | None = None,
    ):
        self.node_id = node_id
        self.command_queue = command_queue

        # persistent state on all servers
        self.current_term = current_term
        self.voted_for = voted_for
        self.replication_log = (
            replication_log if replication_log else RaftReplicationLog()
        )

        # volatile state on all servers
        self.commit_index = 0
        self.last_applied = 0

        # volatile state on leaders
        self.next_index = next_index if next_index else {}
        self.match_index = match_index if match_index else {}

        self.election_timeout = (
            election_timeout if election_timeout else self._set_election_timeout()
        )
        self.cluster_info = (
            cluster_info
            if cluster_info
            else ClusterInfo(cluster_size=len(RAFT_SERVERS), topology=RAFT_SERVERS)
        )
        self.votes_received = votes_received if votes_received else set()

        self.role = StateRole.FOLLOWER

    @abstractmethod
    def on_append_entries(
        self, append_entries: AppendEntries
    ) -> None | AppendEntriesResponse:
        pass

    @abstractmethod
    def on_append_entries_response(
        self, append_entries_response: AppendEntriesResponse
    ) -> None | AppendEntries:
        pass

    @abstractmethod
    def on_vote_request(self, request_vote: RequestVote) -> RequestVoteResponse:
        pass

    @abstractmethod
    def on_vote_response(self, request_vote_response: RequestVoteResponse) -> None:
        pass

    @abstractmethod
    def on_timer_tick(self, clock_tick: ClockTick) -> None | list[RaftRpcMessage]:
        pass

    @abstractmethod
    def on_election_timeout_expiry(self) -> Self | None:
        pass

    @abstractmethod
    def on_leader_election(self) -> list[RequestVote]:
        pass

    @abstractmethod
    def on_log_replication(self, command: str | None) -> list[AppendEntries]:
        pass

    @enforce_invariants
    def handle_message(
        self, message: RaftRpcMessage
    ) -> None | RaftRpcMessage | list[RaftRpcMessage]:
        if isinstance(message, AppendEntries):
            return self.on_append_entries(message)
        elif isinstance(message, AppendEntriesResponse):
            return self.on_append_entries_response(message)
        elif isinstance(message, RequestVote):
            return self.on_vote_request(message)
        elif isinstance(message, RequestVoteResponse):
            return self.on_vote_response(message)
        elif isinstance(message, ClockTick):
            return self.on_timer_tick(message)
        elif isinstance(message, SubmitCommand):
            return self.on_log_replication(command=message.command)

    def step_down_to_follower(self) -> "FollowerState":
        return FollowerState(
            node_id=self.node_id,
            command_queue=self.command_queue,
            current_term=self.current_term,
            voted_for=None,
            replication_log=self.replication_log,
            next_index=self.next_index,
            match_index=self.match_index,
            election_timeout=self.election_timeout,
            cluster_info=self.cluster_info,
        )

    def become_leader(self) -> "LeaderState":
        return LeaderState(
            node_id=self.node_id,
            command_queue=self.command_queue,
            current_term=self.current_term,
            voted_for=None,
            replication_log=self.replication_log,
            next_index=self.next_index,
            match_index=self.match_index,
            election_timeout=self.election_timeout,
            cluster_info=self.cluster_info,
        )

    def _get_prev_log_info(self, follower_id: int) -> PreviousLogInfo:
        prev_log_index = self.next_index[follower_id] - 1
        entries = self.replication_log.get_entries(
            start=prev_log_index,
            end=self.replication_log.last_log_index,
        )
        prev_log_term = entries[0].term

        return PreviousLogInfo(
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries,
        )

    def _build_append_entries(self, follower_id: int) -> AppendEntries:
        prev_log_info = self._get_prev_log_info(follower_id)

        return AppendEntries(
            source_id=self.node_id,
            dest_id=follower_id,
            term=self.current_term,
            prev_log_index=prev_log_info.prev_log_index,
            prev_log_term=prev_log_info.prev_log_term,
            entries=prev_log_info.entries[1:],
            leader_commit=self.commit_index,
        )

    def _resolve_message_term_conflict(
        self, rpc_message: RaftRpcMessage
    ) -> ResultOnTermConflict:
        """
        Resolve term conflicts based on the RaftRpcMessage message.

        Returns:
            ResultOnTermConflict: An enum indicating the result of the term conflict resolution.
            - DROP_MESSAGE: If the received message has a lower term and should be ignored.
            - STEP_DOWN: If the term was updated and the node should step down to follower.
            - NO_OP: If no action is needed.
        """
        if rpc_message.term < self.current_term:
            logging.info(f"Received {rpc_message} with lower term, ignoring...")
            return ResultOnTermConflict.DROP_MESSAGE

        if rpc_message.term > self.current_term:
            self.current_term = rpc_message.term
            self.command_queue.put(RaftCommand.STEP_DOWN_TO_FOLLOWER)

            return ResultOnTermConflict.STEP_DOWN

        return ResultOnTermConflict.NO_OP

    def _reset_election_timeout(self):
        self.election_timeout = self._set_election_timeout()

    @staticmethod
    def _set_election_timeout():
        return ELECTION_TIMEOUT_SECONDS + random.random() * ELECTION_RANDOM

    def _should_grant_vote(self, request_vote: RequestVote) -> bool:
        if request_vote.term < self.current_term:
            return False

        if self.voted_for is None or self.voted_for == request_vote.candidate_id:
            return self.replication_log.is_up_to_date(
                request_vote.last_log_index, request_vote.last_log_term
            )

        return False


class FollowerState(RaftState):
    def __init__(
        self,
        node_id: int,
        command_queue: Queue,
        current_term: int = 0,
        voted_for: int | None = None,
        replication_log: RaftReplicationLog | None = None,
        next_index: dict[int, int] = None,
        match_index: dict[int, int] = None,
        election_timeout: int = 0,
        cluster_info: ClusterInfo | None = None,
        votes_received: set[int] | None = None,
    ):
        super().__init__(
            node_id,
            command_queue,
            current_term,
            voted_for,
            replication_log,
            next_index,
            match_index,
            election_timeout,
            cluster_info,
            votes_received,
        )

    def __repr__(self) -> str:
        return f"FollowerState(node_id={self.node_id}, current_term={self.current_term}, voted_for={self.voted_for})"

    def on_append_entries(
        self, append_entries: AppendEntries
    ) -> None | AppendEntriesResponse:
        logging.info(
            f"Received append_entries request from {append_entries.source_id}..."
        )

        # Check if the term is outdated or if the log doesn't match
        term_outdated = append_entries.term < self.current_term
        log_mismatch = (
            append_entries.prev_log_index not in self.replication_log.entries
            or append_entries.prev_log_term
            != self.replication_log.entries[append_entries.prev_log_index].term
        )

        if term_outdated or log_mismatch:
            return AppendEntriesResponse(
                term=self.current_term,
                success=False,
                source_id=self.node_id,
                dest_id=append_entries.source_id,
                match_index=append_entries.prev_log_index
                + len(append_entries.entries),  # In TLA+ spec
            )

        if self.current_term < append_entries.term:
            self.current_term = append_entries.term
        else:
            # We got an AppendEntries from current leader. Reset election timeout
            self._reset_election_timeout()

        # Append new entries to the log
        is_success = self.replication_log.append_entries(
            prev_index=append_entries.prev_log_index,
            prev_term=append_entries.prev_log_term,
            entries=append_entries.entries,
        )

        # Advance my commit index, but with the caveat that I can
        # not advance it by any more than the log-entries
        # I know about with respect to this AppendEntries request.
        if is_success and append_entries.leader_commit > self.commit_index:
            self.commit_index = min(
                append_entries.leader_commit,
                append_entries.prev_log_index + len(append_entries.entries),
            )

        return AppendEntriesResponse(
            source_id=self.node_id,
            dest_id=append_entries.source_id,
            term=self.current_term,
            success=is_success,
            match_index=append_entries.prev_log_index
            + len(append_entries.entries),  # In TLA+ spec
        )

    def on_append_entries_response(
        self, append_entries_response: AppendEntriesResponse
    ) -> None | AppendEntries:
        result = self._resolve_message_term_conflict(append_entries_response)

        match result:
            case (
                ResultOnTermConflict.STEP_DOWN,
                ResultOnTermConflict.DROP_MESSAGE,
                ResultOnTermConflict.NO_OP,
            ):
                return

    def on_vote_request(self, request_vote: RequestVote) -> RequestVoteResponse:
        if self._should_grant_vote(request_vote):
            self.voted_for = request_vote.source_id

            return RequestVoteResponse(
                term=self.current_term,
                vote_granted=True,
                source_id=self.node_id,
                dest_id=request_vote.source_id,
            )

        return RequestVoteResponse(
            term=self.current_term,
            vote_granted=False,
            source_id=self.node_id,
            dest_id=request_vote.source_id,
        )

    def on_vote_response(self, request_vote_response: RequestVoteResponse) -> None:
        result = self._resolve_message_term_conflict(request_vote_response)

        match result:
            case (
                ResultOnTermConflict.STEP_DOWN,
                ResultOnTermConflict.DROP_MESSAGE,
                ResultOnTermConflict.NO_OP,
            ):
                return

    def on_timer_tick(self, clock_tick: ClockTick) -> None | list[RaftRpcMessage]:
        self.election_timeout -= clock_tick.seconds

        if self.election_timeout < 0:
            self._reset_election_timeout()
            self.command_queue.put(RaftCommand.ELECTION_TIMEOUT_EXPIRED)

        return None

    def on_election_timeout_expiry(self) -> "CandidateState":
        return CandidateState(
            node_id=self.node_id,
            command_queue=self.command_queue,
            current_term=self.current_term,
            voted_for=None,
            replication_log=self.replication_log,
            next_index=self.next_index,
            match_index=self.match_index,
            election_timeout=self.election_timeout,
            cluster_info=self.cluster_info,
        )

    def on_leader_election(self) -> list[RequestVote]:
        raise InvalidStateTransitionError()

    def on_log_replication(self, command: str | None) -> list[AppendEntries]:
        raise InvalidStateTransitionError()


class CandidateState(RaftState):
    def __init__(
        self,
        node_id: int,
        command_queue: Queue,
        current_term: int = 0,
        voted_for: int | None = None,
        replication_log: RaftReplicationLog | None = None,
        next_index: dict[int, int] = None,
        match_index: dict[int, int] = None,
        election_timeout: int = 0,
        cluster_info: ClusterInfo | None = None,
        votes_received: set[int] | None = None,
    ):
        super().__init__(
            node_id,
            command_queue,
            current_term,
            voted_for,
            replication_log,
            next_index,
            match_index,
            election_timeout,
            cluster_info,
            votes_received,
        )
        self.current_term += 1
        self.voted_for = self.node_id

        self.votes_received = {self.node_id}

        self.role = StateRole.CANDIDATE

        self._last_request_vote_second = self._set_last_request_vote_second()

        command_queue.put(RaftCommand.START_LEADER_ELECTION)

    def __repr__(self) -> str:
        return f"CandidateState(node_id={self.node_id}, current_term={self.current_term}, election_timeout={self.election_timeout},voted_for={self.voted_for})"

    def on_append_entries(
        self, append_entries: AppendEntries
    ) -> None | AppendEntriesResponse:
        result = self._resolve_message_term_conflict(append_entries)

        match result:
            case (
                ResultOnTermConflict.STEP_DOWN,
                ResultOnTermConflict.DROP_MESSAGE,
                ResultOnTermConflict.NO_OP,
            ):
                return

    def on_append_entries_response(
        self, append_entries_response: AppendEntriesResponse
    ) -> None | AppendEntries:
        result = self._resolve_message_term_conflict(append_entries_response)

        match result:
            case (
                ResultOnTermConflict.STEP_DOWN,
                ResultOnTermConflict.DROP_MESSAGE,
                ResultOnTermConflict.NO_OP,
            ):
                return

    def on_vote_request(self, request_vote: RequestVote) -> RequestVoteResponse | None:
        if self._should_grant_vote(request_vote):
            self.voted_for = request_vote.source_id

            self._reset_election_timeout()

            return RequestVoteResponse(
                term=self.current_term,
                vote_granted=True,
                source_id=self.node_id,
                dest_id=request_vote.source_id,
            )

        result = self._resolve_message_term_conflict(request_vote)

        match result:
            case ResultOnTermConflict.DROP_MESSAGE:
                return

        return RequestVoteResponse(
            term=self.current_term,
            vote_granted=False,
            source_id=self.node_id,
            dest_id=request_vote.source_id,
        )

    def on_vote_response(self, request_vote_response: RequestVoteResponse) -> None:
        result = self._resolve_message_term_conflict(request_vote_response)

        match result:
            case ResultOnTermConflict.STEP_DOWN, ResultOnTermConflict.DROP_MESSAGE:
                return

        if request_vote_response.vote_granted:
            self.votes_received.add(request_vote_response.source_id)

        if self._is_majority_of_nodes_in_cluster_voted():
            self.command_queue.put(RaftCommand.BECOME_LEADER)

    def on_election_timeout_expiry(self) -> "CandidateState":
        return CandidateState(
            node_id=self.node_id,
            command_queue=self.command_queue,
            current_term=self.current_term,
            voted_for=None,
            replication_log=self.replication_log,
            next_index=self.next_index,
            match_index=self.match_index,
            election_timeout=self.election_timeout,
            cluster_info=self.cluster_info,
        )

    def on_timer_tick(self, clock_tick: ClockTick) -> None | list[RaftRpcMessage]:
        self.election_timeout -= clock_tick.seconds
        is_election_timeout_expired = self.election_timeout < 0

        if is_election_timeout_expired:
            self._reset_election_timeout()
            self.command_queue.put(RaftCommand.ELECTION_TIMEOUT_EXPIRED)

            return

        if self._should_retry_request_vote():
            self._last_request_vote_second = self._set_last_request_vote_second()
            return self.on_leader_election()

        return None

    def on_leader_election(self) -> list[RequestVote]:
        self.voted_for = self.node_id

        request_votes = []

        for follower_id in self.cluster_info.topology:
            if follower_id == self.node_id:
                continue

            if follower_id in self.votes_received:
                continue

            request_votes.append(
                RequestVote(
                    source_id=self.node_id,
                    dest_id=follower_id,
                    term=self.current_term,
                    last_log_index=self.replication_log.last_log_index,
                    last_log_term=self.replication_log.last_log_term,
                    candidate_id=self.node_id,
                )
            )

        return request_votes

    def on_log_replication(self, command: str | None) -> list[AppendEntries]:
        raise InvalidStateTransitionError()

    def _is_majority_of_nodes_in_cluster_voted(self) -> bool:
        return len(self.votes_received) > self.cluster_info.cluster_size // 2

    def _set_last_request_vote_second(self) -> int:
        return int(self.election_timeout) // 2

    def _should_retry_request_vote(self) -> bool:
        is_reached_timeout = int(self.election_timeout) < self._last_request_vote_second
        has_no_majority_vote = not self._is_majority_of_nodes_in_cluster_voted()

        return is_reached_timeout and has_no_majority_vote


class LeaderState(RaftState):
    def __init__(
        self,
        node_id: int,
        command_queue: Queue,
        current_term: int = 0,
        voted_for: int | None = None,
        replication_log: RaftReplicationLog | None = None,
        next_index: dict[int, int] = None,
        match_index: dict[int, int] = None,
        election_timeout: int = 0,
        cluster_info: ClusterInfo | None = None,
        votes_received: set[int] | None = None,
    ):
        super().__init__(
            node_id,
            command_queue,
            current_term,
            voted_for,
            replication_log,
            next_index,
            match_index,
            election_timeout,
            cluster_info,
            votes_received,
        )
        self.next_index = {
            n: self.replication_log.last_log_index + 1
            for n in self.cluster_info.topology.keys()
        }
        self.match_index = {n: 0 for n in self.cluster_info.topology.keys()}
        self.role = StateRole.LEADER

        self.election_timeout = 0
        self._heartbeat_timer = 0
        self._last_heartbeat_second = 0

    def __repr__(self) -> str:
        return f"LeaderState(node_id={self.node_id}, current_term={self.current_term}, voted_for={self.voted_for})"

    def on_append_entries(
        self, append_entries: AppendEntries
    ) -> None | AppendEntriesResponse:
        result = self._resolve_message_term_conflict(append_entries)

        match result:
            case (
                ResultOnTermConflict.STEP_DOWN,
                ResultOnTermConflict.DROP_MESSAGE,
                ResultOnTermConflict.NO_OP,
            ):
                return

    def on_append_entries_response(
        self, append_entries_response: AppendEntriesResponse
    ) -> None | AppendEntries:
        logging.info(
            f"Received append entries response from {append_entries_response.source_id}"
        )

        result = self._resolve_message_term_conflict(append_entries_response)
        match result:
            case ResultOnTermConflict.STEP_DOWN, ResultOnTermConflict.DROP_MESSAGE:
                return

        follower_id = append_entries_response.source_id
        append_entries = None

        if append_entries_response.success:
            self.next_index[follower_id] = append_entries_response.match_index + 1
            self.match_index[follower_id] = append_entries_response.match_index

            self._update_commit_index()

            # If we have more entries in our log, we can immediately update
            # the follower again to send those entries now.
            # See "Rules for Servers: If last log index >= nextIndex
            # for a follower, send AppendEntriesRPC with log entries
            # starting at nextIndex"

            if self.replication_log.last_log_index >= self.next_index[follower_id]:
                return self._build_append_entries(follower_id)
        else:
            logging.info(
                "Failed to replicate log entries, retrying with a smaller index..."
            )

            self.next_index[follower_id] -= 1
            append_entries = self._build_append_entries(follower_id)

        return append_entries

    def on_vote_request(self, request_vote: RequestVote) -> None | RequestVoteResponse:
        result = self._resolve_message_term_conflict(request_vote)

        match result:
            case (
                ResultOnTermConflict.STEP_DOWN,
                ResultOnTermConflict.DROP_MESSAGE,
                ResultOnTermConflict.NO_OP,
            ):
                return

    def on_vote_response(self, request_vote_response: RequestVoteResponse) -> None:
        result = self._resolve_message_term_conflict(request_vote_response)

        match result:
            case (
                ResultOnTermConflict.STEP_DOWN,
                ResultOnTermConflict.DROP_MESSAGE,
                ResultOnTermConflict.NO_OP,
            ):
                return

    def on_timer_tick(self, clock_tick: ClockTick) -> None | list[RaftRpcMessage]:
        self._heartbeat_timer += clock_tick.seconds

        if self._last_heartbeat_second < int(self._heartbeat_timer):
            self._last_heartbeat_second = int(self._heartbeat_timer)
            return self.on_log_replication(command=None)

    def on_election_timeout_expiry(self) -> None:
        raise InvalidStateTransitionError()

    def on_leader_election(self) -> list[RequestVote]:
        raise InvalidStateTransitionError()

    def on_log_replication(self, command: str | None) -> list[AppendEntries]:
        """
        Handles log replication for the leader node.

        If a command is provided, it appends the new command to the log.
        If the command is None, it acts as a signal to send a heartbeat
        to maintain leadership and prevent elections.

        Args:
            command (str | None): The command to be replicated or None for a heartbeat.

        Returns:
            list[AppendEntries]: A list of AppendEntries messages to be sent to followers.
        """
        if command:
            logging.info(f"Handling log replication for leader {self.node_id}")

            self.replication_log.append_new_command(
                leader_term=self.current_term, command=command
            )

        append_entries = []

        for follower_id in self.cluster_info.topology:
            if follower_id == self.node_id:
                continue

            append_entries.append(self._build_append_entries(follower_id))

        return append_entries

    def _update_commit_index(self):
        # The match_index values contain "the truth" about the state
        # of the logs on various followers.  We know that the logs
        # each follower contain at least this many entries that exactly
        # match the leader.

        commit_index = statistics.median_high(self.match_index.values())
        if commit_index > self.commit_index:
            self.commit_index = commit_index


def main():
    def test_suite():
        test_leader_on_log_replication()
        test_follower_on_append_entries_request()
        test_follower_on_outdated_term_append_entries()
        test_follower_on_vote_request()

        test_figure_6_manual_replication(1000)
        test_figure_6_auto_replication(1000)
        test_figure_7_manual_replication(1000)
        test_figure_7_auto_replication(1000)
        # test_voting_figure_6(1000)
        print("All tests passed!")

    cluster_info = ClusterInfo(
        cluster_size=3,
        topology={
            1: ("localhost", 11111),
            2: ("localhost", 22222),
            3: ("localhost", 33333),
        },
    )

    # Leader Scenarios
    def test_leader_on_log_replication():
        cluster_info = ClusterInfo(
            cluster_size=2,
            topology={
                1: ("localhost", 11111),
                2: ("localhost", 22222),
            },
        )

        leader = LeaderState(
            node_id=1, command_queue=Queue(), cluster_info=cluster_info
        )
        follower = FollowerState(
            node_id=2, command_queue=Queue(), cluster_info=cluster_info
        )

        append_entries = leader.on_log_replication("set foo bar")
        assert append_entries == [
            AppendEntries(
                source_id=1,
                dest_id=2,
                term=0,
                prev_log_index=0,
                prev_log_term=0,
                entries=[LogEntry(0, "set foo bar")],
                leader_commit=0,
            ),
        ], "AppendEntries RPCs should be generated for all followers"

        append_entries_response = follower.on_append_entries(append_entries[0])
        assert follower.replication_log.last_log_index == 1, (
            "AppendEntries hasn't been applied correctly"
        )

        # Give the response to the leader
        result = leader.on_append_entries_response(append_entries_response)
        assert result is None

    # Follower Scenarios
    def test_follower_on_append_entries_request():
        follower = FollowerState(
            node_id=1, command_queue=Queue(), cluster_info=cluster_info
        )
        assert follower.on_append_entries(
            AppendEntries(
                source_id=3,
                dest_id=follower.node_id,
                term=0,
                prev_log_index=0,
                prev_log_term=0,
                entries=[LogEntry(0, "set foo bar")],
                leader_commit=0,
            )
        ) == AppendEntriesResponse(
            source_id=follower.node_id, dest_id=3, term=0, success=True, match_index=1
        ), "AppendEntriesResponse doesn't match"

    def test_follower_on_outdated_term_append_entries():
        follower = FollowerState(
            node_id=1, command_queue=Queue(), current_term=1, cluster_info=cluster_info
        )
        assert follower.on_append_entries(
            AppendEntries(
                source_id=3,
                dest_id=follower.node_id,
                term=0,  # Outdated term
                prev_log_index=0,
                prev_log_term=0,
                entries=[LogEntry(0, "set foo bar")],
                leader_commit=0,
            )
        ) == AppendEntriesResponse(
            source_id=follower.node_id, dest_id=3, term=1, success=False, match_index=1
        ), "AppendEntriesResponse should indicate failure due to outdated term"

    def test_follower_on_vote_request():
        follower = FollowerState(
            node_id=1, command_queue=Queue(), current_term=0, cluster_info=cluster_info
        )
        assert follower.on_vote_request(
            RequestVote(
                term=1,
                candidate_id=2,
                last_log_index=0,
                last_log_term=0,
                source_id=2,
                dest_id=1,
            )
        ) == RequestVoteResponse(
            term=0, vote_granted=True, source_id=follower.node_id, dest_id=2
        ), "Follower should grant vote"

    # Possible testing environment.... a fake cluster
    def make_fake_cluster(size: int) -> tuple[ClusterInfo, dict[int, RaftState]]:
        topology = {
            node_id: ("localhost", int(str(node_id) * 5))
            for node_id in range(1, size + 1)
        }
        cluster_info = ClusterInfo(cluster_size=size, topology=topology)

        cluster: dict[int, RaftState] = {
            n: FollowerState(
                node_id=n, command_queue=Queue(), cluster_info=cluster_info
            )
            for n in range(1, size + 1)
        }

        return cluster_info, cluster

    def make_fake_leader(
        cluster_info: ClusterInfo, cluster: dict[int, RaftState], node_id: int
    ) -> None:
        leader = LeaderState(
            node_id=node_id,
            command_queue=Queue(),
            cluster_info=cluster_info,
            replication_log=cluster[node_id].replication_log,
        )
        cluster[node_id] = leader

    def deliver_messages(
        cluster: dict[int, RaftState], messages: list[RaftRpcMessage]
    ) -> list[RaftRpcMessage]:
        import random

        # Deliver messages to a fake cluster, return all outgoing messages
        # that are produced as a result
        outgoing = []

        # Testing ideas: Introduce randomness (see if something breaks). A
        # key part of Raft is that it's supposed to be resilient to
        # bad things happening (dead machines, dropped messages, etc.).
        # So, introducing a bit of "chaos" into testing might be a good idea.
        # This works especially well if you run tests repeatedly.
        # random.shuffle(messages)
        for msg in messages:
            # # Randomly drop a message every so often.
            # if random.random() < 0.0001:
            #     continue

            result = cluster[msg.dest_id].handle_message(msg)

            if result:
                if isinstance(result, RaftRpcMessage):
                    outgoing.append(result)
                else:
                    outgoing.extend(result)

            if not cluster[msg.dest_id].command_queue.empty():
                command = cluster[msg.dest_id].command_queue.get()

                match command:
                    case RaftCommand.STEP_DOWN_TO_FOLLOWER:
                        cluster[msg.dest_id] = cluster[
                            msg.dest_id
                        ].step_down_to_follower()
                    case RaftCommand.START_LEADER_ELECTION:
                        outgoing.extend(cluster[msg.dest_id].on_leader_election())
                    case RaftCommand.BECOME_LEADER:
                        cluster[msg.dest_id] = cluster[msg.dest_id].become_leader()
                        outgoing.extend(
                            cluster[msg.dest_id].on_log_replication(command=None)
                        )
                    case RaftCommand.ELECTION_TIMEOUT_EXPIRED:
                        cluster[msg.dest_id] = cluster[
                            msg.dest_id
                        ].on_election_timeout_expiry()

        return outgoing

    def deliver_all_messages(
        cluster: dict[int, RaftState], messages: list[RaftRpcMessage]
    ) -> None:
        # Deliver messages until there are none to deliver
        while messages:
            messages = deliver_messages(cluster, messages)

    def verify_replication_manual(cluster: dict[int, RaftState]) -> None:
        for _ in range(100):  # Some large number of updates
            append_entries = cluster[1].on_log_replication("set foo bar")
            deliver_all_messages(cluster, append_entries)
            # Check if all logs are the same
            if all(
                node.replication_log == cluster[1].replication_log
                for node in cluster.values()
            ):
                break
        else:
            # Something is wrong. Logs are still different.
            raise AssertionError("Logs not replicating")

    def verify_replication_auto(cluster: dict[int, RaftState]) -> None:
        # Idea here: We set up a fake Raft cluster and hit it
        # with a stream of clock ticks.  It should, all on its own,
        # replicate the log without any other intervention.
        ticks = [
            ClockTick(source_id=n, dest_id=n, seconds=0.5, term=None) for n in cluster
        ]

        for _ in range(100000):  # Some large number of clock ticks
            deliver_all_messages(cluster, ticks)
            # Check if all logs and commit_indices are the same
            if all(
                (
                    node.replication_log == cluster[1].replication_log
                    and node.commit_index == cluster[1].commit_index
                )
                for node in cluster.values()
            ):
                break

        else:
            # Something is wrong. Logs are still different.
            raise AssertionError("Logs not replicating")

    # Cluster e2e tests
    # Possible testing idea.  Recreate some figure from the paper (say Figure 6 or 7).
    # Run various log-replication tests on it in a fake cluster configuration.
    def create_figure_6():
        _, cluster = make_fake_cluster(5)

        def create_log(entries):
            log = RaftReplicationLog()
            for term, cmd in entries:
                log.append_new_command(term, cmd)
            return log

        entries = [
            (1, "x<3"),
            (1, "y<1"),
            (1, "y<9"),
            (2, "x<2"),
            (3, "x<0"),
            (3, "y<7"),
            (3, "x<5"),
            (3, "x<4"),
        ]

        cluster[1].replication_log = create_log(entries)
        cluster[2].replication_log = create_log(entries[:5])
        cluster[3].replication_log = create_log(entries)
        cluster[4].replication_log = create_log(entries[:2])
        cluster[5].replication_log = create_log(entries[:7])

        for n in range(1, 6):
            cluster[n].current_term = 3
        return cluster

    def create_figure_7() -> tuple[ClusterInfo, dict[int, RaftState]]:
        # Although not shown as a cluster, I'll make a 7-node cluster
        # in that configuration.
        def create_log(terms: list[int]):
            log = RaftReplicationLog()
            for term in terms:
                log.append_new_command(term, "")
            return log

        cluster_info, cluster = make_fake_cluster(7)
        cluster[1].replication_log = create_log([1, 1, 1, 4, 4, 5, 5, 6, 6, 6])
        cluster[2].replication_log = create_log([1, 1, 1, 4, 4, 5, 5, 6, 6])
        cluster[3].replication_log = create_log([1, 1, 1, 4])
        cluster[4].replication_log = create_log([1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6])
        cluster[5].replication_log = create_log([1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7])
        cluster[6].replication_log = create_log([1, 1, 1, 4, 4, 4, 4])
        cluster[7].replication_log = create_log([1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3])
        return cluster_info, cluster

    def test_figure_6_manual_replication(n_steps: int):
        # With slight randomness in testing (above), it makes sense to
        # add repetition into testing.  See if it breaks with repeated tests.
        for _ in range(n_steps):
            cluster_info, cluster = create_figure_7()
            make_fake_leader(cluster_info=cluster_info, cluster=cluster, node_id=1)
            verify_replication_manual(cluster)

    def test_figure_6_auto_replication(n_steps: int):
        # With slight randomness in testing (above), it makes sense to
        # add repetition into testing.  See if it breaks with repeated tests.
        for _ in range(n_steps):
            cluster_info, cluster = create_figure_7()
            make_fake_leader(cluster_info=cluster_info, cluster=cluster, node_id=1)
            cluster[1].handle_message(
                SubmitCommand(source_id=1, dest_id=1, term=None, command="x<10")
            )
            verify_replication_auto(cluster)

    def test_figure_7_manual_replication(n_steps: int):
        for _ in range(n_steps):
            cluster_info, cluster = create_figure_7()
            make_fake_leader(cluster_info=cluster_info, cluster=cluster, node_id=1)
            verify_replication_manual(cluster)

    def test_figure_7_auto_replication(n_steps: int):
        for _ in range(n_steps):
            cluster_info, cluster = create_figure_7()
            make_fake_leader(cluster_info=cluster_info, cluster=cluster, node_id=1)
            cluster[1].handle_message(
                SubmitCommand(source_id=1, dest_id=1, term=None, command="x<10")
            )
            verify_replication_auto(cluster)

    def test_voting_figure_6(n_steps: int):
        # Testing concept for leader election.
        # Replicate Figure 6 with no leader.  Try the same replication test.
        # The cluster should have elected a leader and replicated the log.
        # Note: My implementation is based on sending raw "clock ticks" into
        # the logic which then decides what to do with them.  In a cluster
        # with no leader, it should eventually elect a leader and then
        # replicate the log.
        for _ in range(n_steps):
            cluster = create_figure_6()
            verify_replication_auto(cluster)

    test_suite()


if __name__ == "__main__":
    # logging.basicConfig(
    #     level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    # )
    main()
