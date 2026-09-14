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


class DataStream:
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        for item in stream:
            processed = False

            for proc in self.processors:
                if proc.validate(item):
                    proc.ingest(item)
                    processed = True
                    break

            if not processed:
                print(
                    "DataStream error - Can't process element in stream:",
                    item
                )

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")

        if not self.processors:
            print("No processor found, no data")
            return

        for proc in self.processors:
            total = proc.rank + len(proc.data)

            print(
                f"{proc.__class__.__name__}: total {total} items processed, "
                f"remaining {len(proc.data)} on processor"
            )


def main() -> None:
    print("=== Code Nexus - Data Stream ===")
    print("Initialize Data Stream...")

    data_stream = DataStream()

    data_stream.print_processors_stats()

    print("Registering Numeric Processor")

    numeric = NumericProcessor()
    data_stream.register_processor(numeric)

    batch = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {
                "log_level": "WARNING",
                "log_message": "Telnet access! Use ssh instead"
            },
            {
                "log_level": "INFO",
                "log_message": "User wil is connected"
            }
        ],
        42,
        ["Hi", "five"]
    ]

    print("Send first batch of data on stream:", batch)

    data_stream.process_stream(batch)

    data_stream.print_processors_stats()

    print("Registering other data processors")

    text = TextProcessor()
    log = LogProcessor()

    data_stream.register_processor(text)
    data_stream.register_processor(log)

    print("Send the same batch again")

    data_stream.process_stream(batch)

    data_stream.print_processors_stats()

    print(
        "Consume some elements from the data processors: "
        "Numeric 3, Text 2, Log 1"
    )

    for _ in range(3):
        numeric.output()

    for _ in range(2):
        text.output()

    log.output()

    data_stream.print_processors_stats()


if __name__ == "__main__":
    main()
