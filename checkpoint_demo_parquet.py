"""
Demo: Verify checkpoint restart issue when id_column is added in flat_map.
Modified version: Use read_parquet to read data source
Issue: https://github.com/ray-project/ray/issues/60704
"""

import os
import shutil
import tempfile
import time
from typing import Any, Dict, Iterator

import numpy as np
import pyarrow
import pyarrow.parquet

import ray
from ray.data.checkpoint import CheckpointConfig

ID_COLUMN = "unique_id"
NUM_FILES = 4
SAMPLES_PER_FILE = [1, 1, 2, 2]  # Number of samples per file
FAIL_ON_FILE_IDX = 3  # Corresponds to data_003.parquet
FAIL_ON_SAMPLE_IDX = 1


class SimulatedFailure(Exception):
    pass


def create_parquet_data_files(tmp_dir: str) -> str:
    """Create parquet data files with file_id and feature arrays."""
    data_dir = os.path.join(tmp_dir, "parquet_data")
    os.makedirs(data_dir, exist_ok=True)

    for file_idx in range(NUM_FILES):
        file_name = f"data_{file_idx:003d}.parquet"
        file_path = os.path.join(data_dir, file_name)
        num_samples = SAMPLES_PER_FILE[file_idx]

        # Create table with file_id and feature
        file_ids = [f"data_{file_idx:003d}"] * num_samples
        features = np.random.randn(num_samples, 10).tolist()

        table = pyarrow.table(
            {
                "file_id": file_ids,
                "feature": features,
            }
        )
        pyarrow.parquet.write_table(table, file_path)

    return data_dir


def simulate_inference(row: Dict[str, Any]) -> Dict[str, Any]:
    """Simulate model inference, add label."""
    time.sleep(0.5)
    label = np.random.randint(0, 10)
    return {
        ID_COLUMN: row[ID_COLUMN],
        "label": label,
        "info": row["info"],
    }


def expand_samples_and_maybe_fail(
    row: Dict[str, Any], should_fail: bool = True, run_number: int = 1
) -> Iterator[Dict[str, Any]]:
    """Expand samples from each parquet file, add id_column and info column."""
    file_id = row["file_id"]
    file_idx = int(file_id.split("_")[1])
    features = row["feature"]

    for sample_idx in range(len(features)):
        if (
            should_fail
            and file_idx == FAIL_ON_FILE_IDX
            and sample_idx == FAIL_ON_SAMPLE_IDX
        ):
            raise SimulatedFailure(f"Simulated failure: {file_id} sample_{sample_idx}")

        yield {
            ID_COLUMN: f"{file_id}__sample_{sample_idx}",
            "info": f"run_{run_number}",
            "feature": features[sample_idx],
        }


def run_pipeline(
    data_dir: str,
    output_path: str,
    checkpoint_path: str,
    run_number: int,
    enable_failure: bool,
) -> bool:
    print(f"\n=== RUN {run_number} ===")

    ctx = ray.data.DataContext.get_current()
    ctx.checkpoint_config = CheckpointConfig(
        id_column=ID_COLUMN,
        checkpoint_path=checkpoint_path,
        delete_checkpoint_on_success=False,
        skip_read_filter=True,  # ID column is added in flat_map, not in source data
    )

    try:
        # Use read_parquet to read data source
        ds = ray.data.read_parquet(data_dir)

        ds = ds.flat_map(
            expand_samples_and_maybe_fail,
            fn_kwargs={"should_fail": enable_failure, "run_number": run_number},
        )
        ds = ds.map(simulate_inference)

        ds.write_parquet(output_path, concurrency=1)
        print(f"[SUCCESS] Run {run_number} succeeded")
        return True

    except SimulatedFailure:
        print(f"[EXPECTED FAIL] Run {run_number} failed as expected")
        return False

    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {str(e)}")
        return False


def main():
    print(
        "Demo: Verify checkpoint restart issue when id_column is added in flat_map (read_parquet version)"
    )
    print("Issue: https://github.com/ray-project/ray/issues/60704")

    if not ray.is_initialized():
        ray.init(num_cpus=4)

    tmp_dir = tempfile.mkdtemp(prefix="checkpoint_demo_parquet_")
    data_dir = create_parquet_data_files(tmp_dir)
    output_path = os.path.join(tmp_dir, "output")
    checkpoint_path = os.path.join(tmp_dir, "checkpoint")

    if os.path.exists(output_path):
        shutil.rmtree(output_path)

    try:
        # First run: enable failure simulation, fail midway
        success1 = run_pipeline(data_dir, output_path, checkpoint_path, 1, True)

        # Read and display output data
        if os.path.exists(output_path):
            ray.data.DataContext.get_current().checkpoint_config = None
            print("\n=== OUTPUT DATA ===")
            result_ds = ray.data.read_parquet(output_path)
            for row in result_ds.take_all():
                print(row)

        # Second run: disable failure, try to resume from checkpoint
        success2 = run_pipeline(data_dir, output_path, checkpoint_path, 2, False)

        print(f"\nResult: run1={not success1}, run2={success2}")

        # Read and display output data
        if os.path.exists(output_path):
            ray.data.DataContext.get_current().checkpoint_config = None
            print("\n=== OUTPUT DATA ===")
            result_ds = ray.data.read_parquet(output_path)
            for row in result_ds.take_all():
                print(row)

    finally:
        print(f"[INFO] Temp directory: {tmp_dir}")
        ray.shutdown()


if __name__ == "__main__":
    main()
