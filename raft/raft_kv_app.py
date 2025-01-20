import logging


class RaftKvApp:
    def __init__(self):
        self.storage = {}

    def get(self, key: str) -> str:
        return self.storage.get(key, "")

    def set(self, key: str, value: str) -> None:
        self.storage[key] = value

    def delete(self, key: str) -> None:
        self.storage.pop(key, None)

    def run_command(self, command: str):
        logging.info(f"Handling command: {command}")

        match command.split():
            case ["get", key]:
                return self.get(key)
            case ["set", key, value]:
                self.set(key, value)
                return "ok"
            case ["delete", key]:
                self.delete(key)
                return "ok"
            case ["snapshot", filename]:
                self.write_snapshot(filename)
                return "ok"
            case ["restore", filename]:
                self.restore_snapshot(filename)
                return "ok"
            case _:
                return "bad command"

    def write_snapshot(self, filename: str) -> None:
        with open(filename, "w") as f:
            f.write("key,value\n")  # header

            for key, value in self.storage.items():
                f.write(f"{key},{value}\n")

    def restore_snapshot(self, filename: str) -> None:
        with open(filename, "r") as f:
            f.readline()  # header

            for line in f:
                key, value = line.strip("\n").split(",")
                self.set(key, value)


def __test_suite():
    def test_set_and_get():
        app = RaftKvApp()
        app.set("key1", "value1")
        assert app.get("key1") == "value1"

    def test_get_nonexistent_key():
        app = RaftKvApp()
        assert app.get("nonexistent") == ""

    def test_delete_key():
        app = RaftKvApp()
        app.set("key1", "value1")
        app.delete("key1")
        assert app.get("key1") == ""

    def test_run_command_set_and_get():
        app = RaftKvApp()
        assert app.run_command("set key1 value1") == "ok"
        assert app.run_command("get key1") == "value1"

    def test_run_command_delete():
        app = RaftKvApp()
        app.run_command("set key1 value1")
        assert app.run_command("delete key1") == "ok"
        assert app.run_command("get key1") == ""

    test_set_and_get()
    test_get_nonexistent_key()
    test_delete_key()
    test_run_command_set_and_get()
    test_run_command_delete()
    print("All tests passed!")


if __name__ == "__main__":
    __test_suite()
