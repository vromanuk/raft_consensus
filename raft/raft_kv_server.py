import logging
from _socket import SO_REUSEADDR
from concurrent.futures import Future
from socket import AF_INET, SOCK_STREAM, socket
from ssl import SOL_SOCKET
from threading import Thread

from raft.message import recv_message, send_message
from raft.raft_consensus import RaftConsensus
from raft.raft_kv_app import RaftKvApp

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

KV_SERVERS = {
    1: ("localhost", 20000),
    2: ("localhost", 21000),
    3: ("localhost", 22000),
    4: ("localhost", 23000),
    5: ("localhost", 24000),
}


class RaftKvServer:
    def __init__(
        self,
        node_id: int,
        app: RaftKvApp | None = None,
        consensus: RaftConsensus | None = None,
    ) -> None:
        self.node_id = node_id
        self.app = app if app else RaftKvApp()
        self.raft_consensus = (
            consensus
            if consensus
            else RaftConsensus(
                node_id=node_id,
                apply_callback=self.apply_command,
            )
        )

        self.host, self.port = KV_SERVERS[self.node_id]

        self.pending_messages: dict[str, Future] = {}

    def apply_command(self, command: str) -> None:
        # Run the command on the actual application. When Raft
        # decides to "apply the command" to the state machine, it
        # will eventually end up here.  This is step (3) of Figure 1.

        transaction_id, kv_command = command.split(maxsplit=1)
        response = self.app.run_command(kv_command)

        logging.info(f"APPLIED: {command} -> {response}")

        if transaction_id in self.pending_messages:
            self.pending_messages.pop(transaction_id).set_result(response)

        # Note: on followers, the command executes, but there is no
        # client waiting to get the response.  Clients only connect
        # to the leader.

    def run(self):
        self.raft_consensus.run()

        sock = socket(AF_INET, SOCK_STREAM)
        sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
        sock.bind(KV_SERVERS[self.node_id])
        sock.listen()

        logging.info(f"KV-Raft Server {self.node_id} running")

        while True:
            client, _ = sock.accept()
            Thread(target=self.handle_client, args=[client]).start()

    def handle_client(self, client: socket) -> None:
        try:
            while True:
                message = recv_message(client).decode("utf-8")

                transaction_id = message.split()[0]
                future = Future()
                self.pending_messages[transaction_id] = future

                # Step 2: Consensus (raft)
                self.raft_consensus.submit(message)

                try:
                    response = future.result(timeout=5.0)
                except TimeoutError:
                    response = "timeout"
                send_message(client, response.encode("utf-8"))

        except IOError:
            client.close()


def main(node_id: int) -> None:
    kv_server = RaftKvServer(node_id=node_id)
    kv_server.run()


if __name__ == "__main__":
    import sys

    main(node_id=int(sys.argv[1]))
