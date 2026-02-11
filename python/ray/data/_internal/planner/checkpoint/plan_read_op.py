import functools
from typing import Callable, List, Optional

from ray import ObjectRef
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
)
from ray.data._internal.logical.operators import Read
from ray.data._internal.output_buffer import OutputBlockSizeOption
from ray.data._internal.planner.plan_read_op import plan_read_op
from ray.data.checkpoint.util import (
    CHECKPOINTED_IDS_KWARG_NAME,
    filter_checkpointed_rows_for_blocks,
)
from ray.data.context import DataContext


def plan_read_op_with_checkpoint_filter(
    op: Read,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
    load_checkpoint: Optional[Callable[[], ObjectRef]] = None,
) -> PhysicalOperator:
    physical_op = plan_read_op(op, physical_children, data_context)

    # Skip adding the filter if skip_read_filter is set to True.
    # This is useful when the ID column is added later in the pipeline
    # (e.g., in a flat_map or map operator) and doesn't exist in the source data.
    if data_context.checkpoint_config.skip_read_filter:
        return physical_op

    # TODO avoid modifying in-place
    physical_op._map_transformer.add_transform_fns(
        [
            BlockMapTransformFn(
                functools.partial(
                    filter_checkpointed_rows_for_blocks,
                    checkpoint_config=data_context.checkpoint_config,
                ),
                output_block_size_option=OutputBlockSizeOption.of(
                    target_max_block_size=data_context.target_max_block_size,
                ),
            ),
        ]
    )

    if load_checkpoint is not None:
        physical_op.add_map_task_kwargs_fn(
            lambda: {CHECKPOINTED_IDS_KWARG_NAME: load_checkpoint()}
        )

    return physical_op
