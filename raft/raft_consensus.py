import logging
import os
import threading
import time
from collections.abc import Callable
from queue import Queue

from raft.internal.raft_configuration import (
    HEARTBEAT_INTERVAL_SECONDS,
    RAFT_SERVERS,
    TICK_INTERVAL_SECONDS,
)
from raft.internal.raft_entities import ClusterInfo, RaftCommand
from raft.internal.raft_networking import RaftNetwork
from raft.internal.raft_rpc import ClockTick, RaftRpcMessage, SubmitCommand
from raft.internal.raft_state import FollowerState, RaftState

#  What is the overall mental model for how Raft
# is supposed to work?
#
#   (1) Commands are accepted from clients.
#   (2) Commands are replicated across multiple servers.
#   (3) Commands are executed in an identical order on all servers.
#
# Raft does NOT look at the commands or concern itself with
# the meaning of the commands. It only guarantees the replication
# and the ordering of the commands.
#
# My concept: Raft is a kind of "black box" like this:
#
#         submit(command) -> [  Raft  ] -> apply_callback(command)
#
# Applications don't see anything inside. The only guarantee
# is that apply_callback() executes on all servers and
# processes commands in the *SAME* order.   That's it.
# No other guarantees are made.


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class RaftConsensus:
    def __init__(
        self,
        node_id: int,
        command_queue: Queue | None = None,
        state: RaftState | None = None,
        raft_network: RaftNetwork | None = None,
        cluster_info: ClusterInfo | None = None,
        apply_callback: Callable | None = None,
    ):
        self.node_id = node_id
        self.command_queue = command_queue if command_queue else Queue()
        self.state = (
            state
            if state
            else FollowerState(
                node_id=node_id,
                command_queue=self.command_queue,
                cluster_info=cluster_info,
            )
        )
        self.raft_network = raft_network if raft_network else RaftNetwork(node_id)

        self.apply_callback = apply_callback or self._default_apply_command
        self._last_applied = 0

    def run(self) -> None:
        threading.Thread(target=self._execute_messages).start()
        # threading.Thread(target=self._generate_heartbeats).start()
        threading.Thread(target=self._generate_clock_ticks).start()

    def submit(self, command: str) -> None:
        # There must be some way for an application to submit commands to Raft.
        # This is (1) in Figure 1 self.state.submit_command(command)

        # This is extremely subtle, but if you make the
        # Raft logic entirely focused on receiving messages, you will
        # centralize all of its operation into a single place--
        # the "_execute_messages()" method. This looks really weird
        # (sending a message to myself), but it delivers the message
        # to _execute_messages. Because that's the only place that
        # interacts with the logic, you don't have to worry about
        # thread locks and other synchronization.

        self.raft_network.send(
            self.node_id,
            SubmitCommand(
                source_id=self.node_id, dest_id=self.node_id, command=command, term=None
            ),
        )

    def _execute_messages(self) -> None:
        while True:
            msg = self.raft_network.receive()

            result = self.state.handle_message(msg)
            self._send_messages(result)

            while not self.command_queue.empty():
                command = self.command_queue.get()
                actions = self._apply_state_command(command)
                self._send_messages(actions)

            # See if we can tell the application about commands
            if self._last_applied < self.state.commit_index:
                entries = self.state.replication_log.get_entries(
                    self._last_applied + 1, self.state.commit_index
                )

                for entry in entries:
                    self.apply_callback(entry.command)

                self._last_applied = self.state.commit_index

    def _send_messages(
        self, messages: None | RaftRpcMessage | list[RaftRpcMessage]
    ) -> None:
        if not messages:
            return
        if not isinstance(messages, list):
            messages = [messages]
        for message in messages:
            self.raft_network.send(message.dest_id, message)

    def _apply_state_command(
        self, command: RaftCommand
    ) -> list[RaftRpcMessage] | RaftRpcMessage | None:
        """
        Applies a state command to the current Raft state.

        Depending on the command, this method will transition the state of the Raft node
        and may generate one or more RaftRpcMessages as a result of the state change.
        These messages are intended to be forwarded and sent via the `_send_messages` method.

        Args:
            command (RaftCommand): The command to apply to the current state.

        Returns:
            list[RaftRpcMessage] | RaftRpcMessage | None: The messages to be sent as a result
            of the command, or None if no messages are generated.
        """

        match command:
            case RaftCommand.STEP_DOWN_TO_FOLLOWER:
                self.state = self.state.step_down_to_follower()
            case RaftCommand.START_LEADER_ELECTION:
                return self.state.on_leader_election()
            case RaftCommand.BECOME_LEADER:
                self.state = self.state.become_leader()
                return self.state.on_log_replication(command=None)
            case RaftCommand.ELECTION_TIMEOUT_EXPIRED:
                self.state = self.state.on_election_timeout_expiry()

    def _generate_heartbeats(self) -> None:
        try:
            while True:
                time.sleep(HEARTBEAT_INTERVAL_SECONDS)
                append_entries = self.state.on_log_replication(command=None)
                self._send_messages(append_entries)
        except Exception as e:
            logging.error(f"PANIC! run_timer: {e}")
            os._exit(1)

    def _generate_clock_ticks(self) -> None:
        # Different concept. We send periodic clock ticks into
        # the logic without putting any meaning on them.
        while True:
            time.sleep(TICK_INTERVAL_SECONDS)
            self.raft_network.send(
                self.node_id,
                ClockTick(
                    source_id=self.node_id,
                    dest_id=self.node_id,
                    seconds=TICK_INTERVAL_SECONDS,
                    term=None,
                ),
            )

    @staticmethod
    def _default_apply_command(command: str) -> None:
        logging.info(f"APPLYING: {command}")


def run_console(node_id: int, raft_consensus: RaftConsensus) -> None:
    while True:
        cmd = input(f"Raft {node_id} > ")
        match cmd.split(maxsplit=1):
            case ["help"]:
                print(
                    "submit <cmd> - Submit a command to the Raft cluster.\n"
                    "log - Print the current log.\n"
                    "update - Update the current log.\n"
                    "status - Print the current status of the Raft cluster.\n"
                    "exit - Exit the Raft console.\n"
                )
            case ["submit", cmd]:
                raft_consensus.submit(cmd)
            case ["log"]:
                print(raft_consensus.state.replication_log)
            case ["update"]:
                raft_consensus.state.on_log_replication(command="hello, world!")
            case ["status"]:
                print(f"role: {raft_consensus.state}")
                print(
                    f"last_log_index: {raft_consensus.state.replication_log.last_log_index}"
                )
                print(f"current_term: {raft_consensus.state.current_term}")
                print(f"commit_index: {raft_consensus.state.commit_index}")
                print(f"next_index: {raft_consensus.state.next_index}")
                print(f"match_index: {raft_consensus.state.match_index}")

            case ["exit"]:
                os._exit(0)


def main(node_id: int):
    cluster_info = ClusterInfo(cluster_size=5, topology=RAFT_SERVERS)

    consensus = RaftConsensus(node_id=node_id, cluster_info=cluster_info)
    consensus.run()

    run_console(node_id=node_id, raft_consensus=consensus)


if __name__ == "__main__":
    import sys

    main(int(sys.argv[1]))
