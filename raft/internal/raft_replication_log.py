from dataclasses import dataclass

from raft.internal.raft_entities import LogEntry

# Discussion:  At a high-level, the Raft log might appear similar
# to a Python list. However, there are several very annoying differences
# that have led to a lot of programming bugs in past Raft implementation
# efforts.
#
#   (1) The Raft paper uses 1-based indexing throughout.
#       Python (and most other languages) use 0-based indexing.
#
#   (2) The Raft paper uses alot of inclusive endpoints. For
#       example, the "commitIndex" is described as the highest
#       log entry know to be committed.  This is different
#       from Python where ranges typically do *NOT* include
#       the endpoint.  For example, range(1, 10) does NOT include 10.
#
#   (3) Indexing with a negative number in Python does a wrap-around
#       from the end of a list.  Indexing with a negative number
#       in Raft indicates a serious programming bug. There is no
#       log "wrap-around" from the end like that.
#
#   (4) Slicing in Python doesn't care if the slice is out of range.
#       For example, if items=[1,2,3], you can write items[100:1000]
#       and Python will happily return [].  Slicing out of range
#       in Raft almost always indicates a logic bug of some kind.
#
#   (5) Raft features a "log compaction" process where entries
#       at the beginning of the log can be safely discarded.
#       However, log indices must continue to increase.  This is
#       not very list-like.
#
# What I am trying to do below is to create a RaftLog object that
# is extremely strict about edge cases and proper behavior. No
# negative indices. No accessing data out of range. 1-based indexing.
# I'm also intentionally making it very "unpythonic" because I
# want it to be more closely matched to the needs of the Raft paper
# and I don't want accidental programming mistakes.


@dataclass
class PreviousLogInfo:
    prev_log_index: int
    prev_log_term: int
    entries: list[LogEntry]


class RaftReplicationLog:
    def __init__(self):
        self.entries = {0: LogEntry(0, "")}  # Sentinel at index 0
        self.last_log_index = 0

    def __eq__(self, other):
        # Equality check useful for testing
        return (
            self.entries == other.entries
            and self.last_log_index == other.last_log_index
        )

    def __repr__(self):
        return f"RaftLog<last_log_index={self.last_log_index}, {sorted(self.entries.items())}>"

    @property
    def last_log_term(self):
        return self.entries[self.last_log_index].term

    # This operation is performed by leaders (it always works)
    def append_new_command(self, leader_term: int, command: str) -> None:
        entry = LogEntry(leader_term, command)
        self.last_log_index += 1
        self.entries[self.last_log_index] = entry

    # This operation is performed by followers (log must match correctly)
    def append_entries(
        self, prev_index: int, prev_term: int, entries: list[LogEntry]
    ) -> bool:
        # Log is not allowed to have holes in it
        if prev_index not in self.entries:
            return False

        # Term numbers must match correctly
        if self.entries[prev_index].term != prev_term:
            return False

        for index, entry in enumerate(entries, start=prev_index + 1):
            # Figure 2: "If an existing entry conflicts with a new one
            # (same index, but different terms), delete the existing
            # entry and all that follow it."
            if index in self.entries and self.entries[index].term != entry.term:
                self._delete_all_entries_from(index)

            self.entries[index] = entry
            self.last_log_index = max(index, self.last_log_index)

        return True

    def _delete_all_entries_from(self, index: int) -> None:
        self.last_log_index = index - 1
        while self.entries.pop(index, None):
            index += 1

    def get_entries(self, start: int, end: int) -> list[LogEntry]:
        # Get log entries within a range of indices (inclusive)
        return [self.entries[n] for n in range(start, end + 1)]

    def is_up_to_date(self, last_log_index: int, last_log_term: int) -> bool:
        if last_log_term < self.last_log_term:
            return False

        if last_log_index < self.last_log_index:
            return False

        return True


def test_raftlog():
    log = RaftReplicationLog()
    # Leader appends should always just work
    log.append_new_command(1, "x")
    log.append_new_command(1, "y")
    assert log.get_entries(1, 2) == [
        LogEntry(1, "x"),
        LogEntry(1, "y"),
    ]

    # Test various follower-appends
    log = RaftReplicationLog()
    assert (
        log.append_entries(
            prev_index=0,
            prev_term=0,
            entries=[LogEntry(1, "x")],
        )
        == True
    )
    assert (
        log.append_entries(
            prev_index=1,
            prev_term=1,
            entries=[LogEntry(1, "y")],
        )
        == True
    )
    assert log.get_entries(1, 2) == [
        LogEntry(1, "x"),
        LogEntry(1, "y"),
    ]

    # Repeated operations should be ok (idempotent)
    assert (
        log.append_entries(
            prev_index=0,
            prev_term=0,
            entries=[LogEntry(1, "x")],
        )
        == True
    )
    assert (
        log.append_entries(
            prev_index=1,
            prev_term=1,
            entries=[LogEntry(1, "y")],
        )
        == True
    )
    assert log.get_entries(1, 2) == [
        LogEntry(1, "x"),
        LogEntry(1, "y"),
    ]

    # Can't have holes
    assert (
        log.append_entries(
            prev_index=10,
            prev_term=1,
            entries=[LogEntry(1, "z")],
        )
        == False
    )

    # Can't append if the previous term doesn't match up right
    assert (
        log.append_entries(
            prev_index=2,
            prev_term=2,
            entries=[LogEntry(2, "z")],
        )
        == False
    )

    # If adding an entry with a conflicting term, all entries afterwards should
    # go away
    assert (
        log.append_entries(
            prev_index=0,
            prev_term=0,
            entries=[LogEntry(3, "w")],
        )
        == True
    )
    assert log.last_log_index == 1

    # Adding empty entries should work as log as term/index is okay
    assert log.append_entries(prev_index=1, prev_term=3, entries=[]) == True
    assert log.append_entries(prev_index=1, prev_term=2, entries=[]) == False

    print("All tests passed!")


if __name__ == "__main__":
    test_raftlog()
