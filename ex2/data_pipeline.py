import abc
import typing


class DataProcessor(abc.ABC):
    def __init__(self) -> None:
        self.data: list[str] = []
        self.rank: int = 0

    @abc.abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abc.abstractmethod
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
                if (
                    not isinstance(item, (int, float))
                    or isinstance(item, bool)
                ):
                    return False
            return True

        return False

    def ingest(
        self, data: int | float | list[int | float]
    ) -> None:
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
                    if (
                        not isinstance(key, str)
                        or not isinstance(value, str)
                    ):
                        return False

            return True

        return False

    def ingest(
        self, data: dict[str, str] | list[dict[str, str]]
    ) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")

        if isinstance(data, list):
            values = data
        else:
            values = [data]

        for item in values:
            log_entry = (
                f"{item['log_level']}: {item['log_message']}"
            )
            self.data.append(log_entry)


class ExportPlugin(typing.Protocol):
    def process_output(
        self, data: list[tuple[int, str]]
    ) -> None:
        ...


class CSVExport:
    def process_output(
        self, data: list[tuple[int, str]]
    ) -> None:
        values = [value for _, value in data]

        print("CSV Output:")
        print(",".join(values))


class JSONExport:
    def process_output(
        self, data: list[tuple[int, str]]
    ) -> None:
        items = []

        for index, value in data:
            items.append(f'"item_{index}": "{value}"')

        result = "{" + ", ".join(items) + "}"

        print("JSON Output:")
        print(result)


class DataStream:
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(
        self, stream: list[typing.Any]
    ) -> None:
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
                f"{proc.__class__.__name__}: "
                f"total {total} items processed, "
                f"remaining {len(proc.data)} on processor"
            )

    def output_pipeline(
        self, nb: int, plugin: ExportPlugin
    ) -> None:
        for proc in self.processors:
            data: list[tuple[int, str]] = []

            for _ in range(nb):
                try:
                    data.append(proc.output())
                except IndexError:
                    break

            if data:
                plugin.process_output(data)


def main() -> None:
    print("=== Code Nexus - Data Pipeline ===")
    print("Initialize Data Stream...")

    data_stream = DataStream()

    data_stream.print_processors_stats()

    print("Registering Processors")

    numeric = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()

    data_stream.register_processor(numeric)
    data_stream.register_processor(text)
    data_stream.register_processor(log)

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

    print("Send 3 processed data from each processor to a CSV plugin:")

    csv_plugin = CSVExport()
    data_stream.output_pipeline(3, csv_plugin)

    data_stream.print_processors_stats()

    batch2 = [
        21,
        ["I love AI", "LLMs are wonderful", "Stay healthy"],
        [
            {
                "log_level": "ERROR",
                "log_message": "500 server crash"
            },
            {
                "log_level": "NOTICE",
                "log_message": "Certificate expires in 10 days"
            }
        ],
        [32, 42, 64, 84, 128, 168],
        "World hello"
    ]

    print("Send another batch of data:", batch2)

    data_stream.process_stream(batch2)

    data_stream.print_processors_stats()

    print("Send 5 processed data from each processor to a JSON plugin:")

    json_plugin = JSONExport()
    data_stream.output_pipeline(5, json_plugin)

    data_stream.print_processors_stats()


if __name__ == "__main__":
    main()
