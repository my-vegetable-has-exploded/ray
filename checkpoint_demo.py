"""
Demo: 验证 id_column 在 flat_map 中添加时的 checkpoint restart 问题。
Issue: https://github.com/ray-project/ray/issues/60704
"""

import os
import shutil
import tempfile
import time
from typing import Any, Dict, Iterator

import numpy as np

import ray
from ray.data.checkpoint import CheckpointConfig

ID_COLUMN = "unique_id"
NPZ_FILE_SAMPLES = {
    "data_000.npz": 3,
    "data_001.npz": 1,
    "data_002.npz": 2,
    "data_003.npz": 2,
    "data_004.npz": 1,
    "data_005.npz": 4,
}
FAIL_ON_FILE = "data_005.npz"
FAIL_ON_SAMPLE_IDX = 3


class SimulatedFailure(Exception):
    pass


def create_npz_data_files(tmp_dir: str) -> str:
    """创建 npz 数据文件，直接写入 feature 数组。"""
    data_dir = os.path.join(tmp_dir, "npz_data")
    os.makedirs(data_dir, exist_ok=True)

    for file_name, num_samples in NPZ_FILE_SAMPLES.items():
        file_path = os.path.join(data_dir, file_name)
        features = np.random.randn(num_samples, 10)
        np.savez(file_path, feature=features)

    return data_dir


def simulate_inference(row: Dict[str, Any]) -> Dict[str, Any]:
    """模拟模型推理，添加 label，只保留 ID_COLUMN、label、info、feature。"""
    time.sleep(0.5)
    label = np.random.randint(0, 10)
    return {
        ID_COLUMN: row[ID_COLUMN],
        "label": label,
        "info": row["info"],
    }


def extract_samples_and_maybe_fail(
    row: Dict[str, Any], should_fail: bool = True, run_number: int = 1
) -> Iterator[Dict[str, Any]]:
    """展开 npz 文件，直接读取 feature 数组，添加 id_column 和 info 列。"""
    file_path = row["path"]
    file_name = os.path.basename(file_path)
    data = np.load(file_path)

    features = data["feature"]  # shape: (num_samples, 10)
    for sample_idx in range(len(features)):
        if (
            should_fail
            and file_name == FAIL_ON_FILE
            and sample_idx == FAIL_ON_SAMPLE_IDX
        ):
            data.close()
            raise SimulatedFailure(f"模拟失败: {file_name} sample_{sample_idx}")

        yield {
            ID_COLUMN: f"{file_name}__sample_{sample_idx}",
            "info": f"run_{run_number}",
            "feature": features[sample_idx].tolist(),
        }

    data.close()


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
    )

    try:
        npz_files = sorted(
            [
                os.path.join(data_dir, f)
                for f in os.listdir(data_dir)
                if f.endswith(".npz")
            ]
        )
        ds = ray.data.from_items([{"path": p} for p in npz_files])

        ds = ds.flat_map(
            extract_samples_and_maybe_fail,
            fn_kwargs={"should_fail": enable_failure, "run_number": run_number},
        )
        ds = ds.map(simulate_inference)

        ds.write_parquet(output_path, concurrency=1)
        print(f"[SUCCESS] 运行 {run_number} 成功")
        return True

    except SimulatedFailure:
        print(f"[EXPECTED FAIL] 运行 {run_number} 按预期失败")
        return False

    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {str(e)}")
        return False


def main():
    print("Demo: 验证 id_column 在 flat_map 中添加时的 checkpoint restart 问题")
    print("Issue: https://github.com/ray-project/ray/issues/60704")

    if not ray.is_initialized():
        ray.init(num_cpus=4)

    tmp_dir = tempfile.mkdtemp(prefix="checkpoint_demo_")
    data_dir = create_npz_data_files(tmp_dir)
    output_path = os.path.join(tmp_dir, "output")
    checkpoint_path = os.path.join(tmp_dir, "checkpoint")

    if os.path.exists(output_path):
        shutil.rmtree(output_path)

    try:
        # 第一次运行：启用失败模拟，中途失败
        success1 = run_pipeline(data_dir, output_path, checkpoint_path, 1, True)

        # 读取输出数据展示
        if os.path.exists(output_path):
            ray.data.DataContext.get_current().checkpoint_config = None
            print("\n=== 输出数据 ===")
            result_ds = ray.data.read_parquet(output_path)
            for row in result_ds.take_all():
                print(row)

        # 第二次运行：禁用失败，尝试从 checkpoint 恢复
        success2 = run_pipeline(data_dir, output_path, checkpoint_path, 2, False)

        print(f"\n结果: 第一次={not success1}, 第二次={success2}")

        # 读取输出数据展示
        if os.path.exists(output_path):
            ray.data.DataContext.get_current().checkpoint_config = None
            print("\n=== 输出数据 ===")
            result_ds = ray.data.read_parquet(output_path)
            for row in result_ds.take_all():
                print(row)

    finally:
        print(f"[INFO] 临时目录: {tmp_dir}")
        ray.shutdown()


if __name__ == "__main__":
    main()
