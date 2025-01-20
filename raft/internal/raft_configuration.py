RAFT_SERVERS = {
    1: ("localhost", 15000),
    2: ("localhost", 16000),
    3: ("localhost", 17000),
    4: ("localhost", 18000),
    5: ("localhost", 19000),
}

HEARTBEAT_INTERVAL_SECONDS = 2
TICK_INTERVAL_SECONDS = 0.1

ELECTION_TIMEOUT_SECONDS = 5  # Minimum election timeout
ELECTION_RANDOM = 5  # Amount of randomness in elections
