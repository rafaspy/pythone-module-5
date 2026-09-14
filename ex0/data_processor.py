from abc import ABC, abstractmethod
import typing


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.data: list[str] = []
        self.rank: int = 0

    @abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self.data:
            raise IndexError("No data available")
        value = self.data.pop(0)
        result = (self.rank, value)
        self.rank += 1
        return result


class NumericProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, (int, float)) and not isinstance(data, bool):
            return True

        if isinstance(data, list):
            for item in data:
                if (not isinstance(item, (int, float))
                        or isinstance(item, bool)):
                    return False
            return True

        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")

        if isinstance(data, list):
            values = data
        else:
            values = [data]
        for item in values:
            self.data.append(str(item))


class TextProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, str):
            return True

        if isinstance(data, list):
            for item in data:
                if not isinstance(item, str):
                    return False
            return True

        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")

        if isinstance(data, list):
            values = data
        else:
            values = [data]
        for item in values:
            self.data.append(item)


class LogProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, dict):
            for key, value in data.items():
                if not isinstance(key, str) or not isinstance(value, str):
                    return False
            return True

        if isinstance(data, list):
            for item in data:
                if not isinstance(item, dict):
                    return False

                for key, value in item.items():
                    if not isinstance(key, str) or not isinstance(value, str):
                        return False

            return True
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")

        if isinstance(data, list):
            values = data
        else:
            values = [data]
        for item in values:
            log_entry = str(item)
            self.data.append(log_entry)


def main() -> None:
    numeric = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()

    print("=== Code Nexus - Data Processor ===")

    # NUMERIC PROCESSOR
    print("\nTesting Numeric Processor...")
    print("Trying to validate input '42':", numeric.validate(42))
    print("Trying to validate input 'Hello':", numeric.validate("Hello"))
    try:
        print("Test invalid ingestion of string 'foo' "
              "without prior validation:")
        numeric.ingest("foo")
    except ValueError as error:
        print(f"Got exception: {error}")

    numeric.ingest([1, 2, 3, 4, 5])

    for _ in range(3):
        rank, value = numeric.output()
        print(f"Numeric  value {rank}: {value}")

    # TEXT PROCESSOR
    print("\nTesting Text Processor...")
    print("Trying to validate input '42':", text.validate(42))
    print(
        "Trying to validate input ['Hello', 'Nexus', 'World']:",
        text.validate(["Hello", "Nexus", "World"])
    )

    text.ingest(["Hello", "Nexus", "World"])

    for _ in range(3):
        rank, value = text.output()
        print(f"Text value {rank}: {value}")

    # LOG PROCESSOR
    print("\nTesting Log Processor...")
    print("Trying to validate input 'Hello':", log.validate("Hello"))

    logs = [
        {
            "user": "Rafael",
            "action": "login"
        },
        {
            "log_level": "ERROR",
            "log_message": "Unauthorized access!!"
        }
    ]

    log.ingest(logs)

    for _ in range(2):
        rank, value = log.output()
        print(f"Log entry {rank}: {value}")


if __name__ == "__main__":
    main()
