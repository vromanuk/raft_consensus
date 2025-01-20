import uuid
from socket import AF_INET, SOCK_STREAM, socket

from raft.message import recv_message, send_message
from raft.raft_kv_server import KV_SERVERS


def main(node_id: int) -> None:
    sock = socket(AF_INET, SOCK_STREAM)
    sock.connect(KV_SERVERS[node_id])

    last_command = ""

    while True:
        msg = input(f"KV {node_id} > ")
        if not msg:
            break
        if msg == "retry":
            command = last_command
        else:
            last_command = command = f"{uuid.uuid4()} {msg}"
        send_message(sock, command.encode("utf-8"))
        response = recv_message(sock)
        print("Received >", response.decode("utf-8"))

    sock.close()


if __name__ == "__main__":
    import sys

    main(node_id=(int(sys.argv[1])))
