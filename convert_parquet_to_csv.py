from __future__ import annotations

import argparse
import csv
import io
import multiprocessing
import os
import queue
import sys
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from decimal import Decimal
from importlib import import_module
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
DEFAULT_DELIMITER = ","
DEFAULT_DECIMAL_SEPARATOR = "."
DEFAULT_BATCH_SIZE = 5_000
DEFAULT_MAX_PENDING_TASKS_PER_WORKER = 2


def parse_args() -> argparse.Namespace:
    default_workers = max(1, os.cpu_count() or 1)
    parser = argparse.ArgumentParser(
        description=(
            "Searches the 'input' directory for Parquet files and converts them "
            "to CSV files in the 'output' directory."
        )
    )
    parser.add_argument(
        "--delimiter",
        default=DEFAULT_DELIMITER,
        help="CSV delimiter, for example ',' or ';' (default: ',').",
    )
    parser.add_argument(
        "--decimal",
        default=DEFAULT_DECIMAL_SEPARATOR,
        help="Decimal separator for numeric values, for example '.' or ',' (default: '.').",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=default_workers,
        help=(
            "Number of worker processes to use for reading and converting the current file "
            f"(default: {default_workers})."
        ),
    )

    args = parser.parse_args()

    if len(args.delimiter) != 1:
        parser.error("--delimiter must be exactly one character long.")

    if len(args.decimal) != 1:
        parser.error("--decimal must be exactly one character long.")

    if args.delimiter == args.decimal:
        parser.error("--delimiter and --decimal must not be identical.")

    if args.workers < 1:
        parser.error("--workers must be at least 1.")

    return args


def format_value(value: object, decimal_separator: str) -> str:
    if value is None:
        return ""

    if isinstance(value, (float, Decimal)):
        return str(value).replace(".", decimal_separator)

    return str(value)


def render_csv_chunk(
    parquet_file_path: str,
    row_group_index: int,
    delimiter: str,
    decimal_separator: str,
    progress_queue: object | None = None,
) -> tuple[str, int]:
    pq = import_module("pyarrow.parquet")
    parquet_data = pq.ParquetFile(parquet_file_path)
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, delimiter=delimiter)

    row_count = 0

    for batch in parquet_data.iter_batches(
        batch_size=DEFAULT_BATCH_SIZE,
        row_groups=[row_group_index],
        use_threads=True,
    ):
        columns = batch.to_pydict()
        column_names = list(batch.schema.names)

        for row_index in range(batch.num_rows):
            writer.writerow(
                [format_value(columns[column_name][row_index], decimal_separator) for column_name in column_names]
            )

        row_count += batch.num_rows
        if progress_queue is not None:
            progress_queue.put(batch.num_rows)

    return buffer.getvalue(), row_count


def drain_progress_queue(
    progress_queue: object,
    processed_rows: int,
    total_rows: int,
    progress: object,
    file_task_id: int,
) -> int:
    while True:
        try:
            processed_rows += progress_queue.get_nowait()
        except queue.Empty:
            break

    progress.update(file_task_id, completed=min(processed_rows, total_rows))
    return processed_rows


def flush_completed_tasks(
    output_file: io.TextIOBase,
    completed_tasks: dict[int, tuple[str, int]],
    next_task_to_write: int,
    written_rows: int,
) -> tuple[int, int]:
    while next_task_to_write in completed_tasks:
        csv_chunk, row_count = completed_tasks.pop(next_task_to_write)
        output_file.write(csv_chunk)
        written_rows += row_count
        next_task_to_write += 1

    return next_task_to_write, written_rows


def load_parquet_module() -> object:
    try:
        return import_module("pyarrow.parquet")
    except ImportError:
        print(
            "Missing dependency: pyarrow. Install it with 'pip install pyarrow'.",
            file=sys.stderr,
        )
        raise SystemExit(1)


def load_rich_progress_components() -> tuple[object, object, object, object, object, object, object, object, object]:
    try:
        rich_live = import_module("rich.live")
        rich_progress = import_module("rich.progress")
        rich_console = import_module("rich.console")
    except ImportError:
        print(
            "Missing dependency: rich. Install it with 'pip install rich'.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    return (
        rich_console.Console,
        rich_console.Group,
        rich_live.Live,
        rich_progress.Progress,
        rich_progress.SpinnerColumn,
        rich_progress.TextColumn,
        rich_progress.BarColumn,
        rich_progress.TaskProgressColumn,
        rich_progress.TimeElapsedColumn,
    )


def write_parquet_to_csv(
    parquet_file: Path,
    csv_path: Path,
    pq: object,
    delimiter: str,
    decimal_separator: str,
    executor: ProcessPoolExecutor,
    max_workers: int,
    progress_queue: object,
    status_progress: object,
    status_task_id: int,
    progress: object,
    total_task_id: int,
    file_task_id: int,
    file_index: int,
    file_total: int,
    display_name: str,
) -> None:
    parquet_data = pq.ParquetFile(parquet_file)
    metadata = parquet_data.metadata
    total_rows = metadata.num_rows
    total_row_groups = metadata.num_row_groups
    processed_rows = 0
    written_rows = 0
    max_pending_tasks = max(2, max_workers * DEFAULT_MAX_PENDING_TASKS_PER_WORKER)

    status_progress.update(
        status_task_id,
        description=f"Current file ({file_index}/{file_total}): {display_name}",
    )
    progress.update(
        file_task_id,
        description="File progress",
        total=max(total_rows, 1),
        completed=0,
        visible=True,
    )
    progress.update(total_task_id, description="Total progress")

    with csv_path.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.writer(output_file, delimiter=delimiter)
        pending_futures: dict[object, int] = {}
        completed_tasks: dict[int, tuple[str, int]] = {}
        next_task_to_write = 0

        if total_rows == 0:
            writer.writerow(parquet_data.schema_arrow.names)
            progress.update(file_task_id, completed=1)
            return

        writer.writerow(parquet_data.schema_arrow.names)

        for row_group_index in range(total_row_groups):
            future = executor.submit(
                render_csv_chunk,
                str(parquet_file),
                row_group_index,
                delimiter,
                decimal_separator,
                progress_queue,
            )
            pending_futures[future] = row_group_index

            if len(pending_futures) >= max_pending_tasks:
                while len(pending_futures) >= max_pending_tasks:
                    done_futures, _ = wait(
                        pending_futures,
                        timeout=0.1,
                        return_when=FIRST_COMPLETED,
                    )
                    processed_rows = drain_progress_queue(
                        progress_queue,
                        processed_rows,
                        total_rows,
                        progress,
                        file_task_id,
                    )

                    for done_future in done_futures:
                        task_index = pending_futures.pop(done_future)
                        completed_tasks[task_index] = done_future.result()

                    next_task_to_write, written_rows = flush_completed_tasks(
                        output_file,
                        completed_tasks,
                        next_task_to_write,
                        written_rows,
                    )

        while pending_futures:
            done_futures, _ = wait(
                pending_futures,
                timeout=0.1,
                return_when=FIRST_COMPLETED,
            )
            processed_rows = drain_progress_queue(
                progress_queue,
                processed_rows,
                total_rows,
                progress,
                file_task_id,
            )

            for done_future in done_futures:
                task_index = pending_futures.pop(done_future)
                completed_tasks[task_index] = done_future.result()

        next_task_to_write, written_rows = flush_completed_tasks(
            output_file,
            completed_tasks,
            next_task_to_write,
            written_rows,
        )
        processed_rows = drain_progress_queue(
            progress_queue,
            processed_rows,
            total_rows,
            progress,
            file_task_id,
        )


def convert_parquet_files(
    input_dir: Path,
    output_dir: Path,
    delimiter: str,
    decimal_separator: str,
    workers: int,
) -> int:
    pq = load_parquet_module()
    Console, Group, Live, Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn = (
        load_rich_progress_components()
    )
    parquet_files = sorted(input_dir.rglob("*.parquet"))

    if not parquet_files:
        print(f"No Parquet files found in '{input_dir}'.")
        return 0

    output_dir.mkdir(parents=True, exist_ok=True)

    converted_count = 0
    console = Console()
    max_workers = workers
    status_progress = Progress(
        TextColumn("{task.description}"),
        console=console,
        transient=False,
    )
    progress = Progress(
        TextColumn("{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
        transient=False,
    )

    group = Group(status_progress, progress)

    with multiprocessing.Manager() as manager:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            with Live(group, console=console, refresh_per_second=10):
                status_task_id = status_progress.add_task("Current file:", total=None)
                file_task_id = progress.add_task("File progress", total=1)
                total_task_id = progress.add_task("Total progress", total=len(parquet_files))

                for file_index, parquet_file in enumerate(parquet_files, start=1):
                    relative_path = parquet_file.relative_to(input_dir)
                    csv_path = output_dir / relative_path.with_suffix(".csv")
                    csv_path.parent.mkdir(parents=True, exist_ok=True)
                    progress_queue = manager.Queue()

                    write_parquet_to_csv(
                        parquet_file,
                        csv_path,
                        pq,
                        delimiter,
                        decimal_separator,
                        executor,
                        max_workers,
                        progress_queue,
                        status_progress,
                        status_task_id,
                        progress,
                        total_task_id,
                        file_task_id,
                        file_index,
                        len(parquet_files),
                        relative_path.as_posix(),
                    )

                    progress.advance(total_task_id, 1)
                    converted_count += 1

                status_progress.update(status_task_id, description="Current file: completed")
                progress.update(file_task_id, completed=1, total=1)

    return converted_count


def main() -> int:
    args = parse_args()

    if not INPUT_DIR.exists():
        print(f"Input directory '{INPUT_DIR}' does not exist.", file=sys.stderr)
        return 1

    converted_count = convert_parquet_files(
        INPUT_DIR,
        OUTPUT_DIR,
        args.delimiter,
        args.decimal,
        args.workers,
    )
    print(f"Done. {converted_count} file(s) converted.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())