import logging
import threading
from _socket import SO_REUSEADDR
from queue import Queue
from socket import AF_INET, SOCK_STREAM, socket
from ssl import SOL_SOCKET

from raft.internal.raft_configuration import RAFT_SERVERS
from raft.internal.raft_rpc import RaftRpcMessage, decode_message, encode_message
from raft.message import recv_message, send_message

# An implementation of the Raft networking layer.  Raft consists of
# a cluster of identical servers.  The servers must be able to
# communicate with each other in some way.  Otherwise, you don't
# have a distributed system.

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class RaftNetwork:
    """
     Represents the networking layer for a node in a Raft cluster.

     This class handles the sending and receiving of messages between nodes
     in a Raft distributed system. Each node is identified by a unique node ID,
     and communication is facilitated through sockets. Incoming and outgoing
     messages are managed using queues.

    The mental model for sending messages:
     - The `send` method is used to send a message from the current node to a specified receiver node.
     - Outgoing messages are queued in a dedicated queue for each receiver node.
     - A separate thread is spawned for each receiver node to handle the delivery of messages from the queue.

     The mental model for receiving messages:
     - The `receive` method retrieves messages sent to the current node.
     - Incoming messages are stored in a single queue.
     - A single receiver thread is responsible for accepting connections and reading incoming messages into the queue.

     Attributes:
         node_id (int): The unique identifier for the node in the Raft cluster.
         host (str): The host address of the node.
         port (int): The port number of the node.
         incoming_messages (Queue): Queue to store incoming messages.
         outgoing_messages (dict[int, Queue]): Dictionary mapping node IDs to queues for outgoing messages.
    """

    def __init__(
        self,
        node_id: int,
        incoming_messages: Queue | None = None,
        outgoing_messages: dict[int, Queue] | None = None,
    ):
        self.node_id = node_id

        self.host, self.port = RAFT_SERVERS[node_id]

        self.incoming_messages = Queue() if not incoming_messages else incoming_messages
        self.outgoing_messages = outgoing_messages if outgoing_messages else {}

        self._is_receiver_running = False

    def send(self, destination_node_id: int, message: RaftRpcMessage) -> None:
        """
        Sends a message to a specified receiver node in the Raft cluster.

        If the destination is the current node, the message is added to the incoming queue.
        Otherwise, a thread is spawned to handle message delivery to the receiver's queue.

        Args:
            destination_node_id (int): The receiver node's ID.
            message (bytes): The message to send.
        """

        if destination_node_id == self.node_id:
            self.incoming_messages.put(message)
            return

        if destination_node_id not in self.outgoing_messages:
            self.outgoing_messages[destination_node_id] = Queue()
            threading.Thread(
                target=self._send_messages, args=[destination_node_id]
            ).start()

        self.outgoing_messages[destination_node_id].put(message)

    def _send_messages(self, destination_node_id: int) -> None:
        sock = None
        while True:
            msg = self.outgoing_messages[destination_node_id].get()
            # best-effort delivery, so we don't care if the message fails to send
            try:
                if sock is None:
                    sock = socket(AF_INET, SOCK_STREAM)
                    sock.connect(RAFT_SERVERS[destination_node_id])
                send_message(sock, encode_message(msg))
            except IOError as _:
                sock = None

    def receive(self) -> RaftRpcMessage:
        """
        Retrieves a message sent to the current node from the incoming queue.

        This method ensures that messages are processed in the order they are received.
        A single dedicated thread is responsible for accepting connections and reading
        incoming messages into the queue.

        Returns:
            RaftRpcMessage: The next message from the incoming queue.
        """

        if not self._is_receiver_running:
            self._is_receiver_running = True
            threading.Thread(target=self._start_receiver_thread).start()

        return self.incoming_messages.get()

    # Thread to accept incoming connections and read messages
    def _start_receiver_thread(self):
        sock = socket(AF_INET, SOCK_STREAM)
        sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
        sock.bind(RAFT_SERVERS[self.node_id])
        sock.listen()
        while True:
            client, _ = sock.accept()
            threading.Thread(target=self._receive_messages, args=[client]).start()

    def _receive_messages(self, sock: socket) -> None:
        try:
            while True:
                msg = decode_message(recv_message(sock))
                self.incoming_messages.put(msg)
        except IOError as _:
            sock.close()
            return


def simulator(node_id: int):
    network = RaftNetwork(node_id)

    def receiver():
        while True:
            msg = network.receive()
            print("Received:", msg)

    threading.Thread(target=receiver).start()

    while True:
        cmd = input(f"Node {node_id} > ")
        try:
            dest, message = cmd.split()
            network.send(int(dest), message.encode("utf-8"))
        except ValueError:
            pass


if __name__ == "__main__":
    import sys

    simulator(int(sys.argv[1]))
