## ray-shuffle

实现高性能shuffle的核心优化：
1. 任务协调：协调mapper和reducer任务的放置与执行、合并中间结果。
2. 高效数据传输：将I/O和计算任务pipeline，支持数据spill到磁盘中。
3. 容错：通过重试或复制确保数据可靠传输到reducer。

In this work, we extend Ray with the necessary features to support large-scale shuffle (Section 4). These include: (1) locality scheduling primitives to enable colocating tasks to better exploit shuffle data locality; (2) a full distributed memory hierarchy with disk spilling and recovery; (3) asynchronous object fetching to pipeline task execution with disk and network I/O. We present Exoshuffle, a flexible and scalable library for distributed shuffle built on top of Ray.

## 背景

在包含 M 个map任务和 R 个reduce任务的 MapReduce 操作中，洗牌过程会产生 M×R 个中间块。这些块中的每一个都必须在内存、磁盘和网络之间移动。随着任务数量的增加，块的数量呈二次增长，而块的大小则呈二次减少，这可能导致大量非常小的块。对 I/O 效率构成了巨大挑战。一些系统通过合并中间块、pipeline来优化 I/O 效率，但是系统的实现方法大多伴随着高昂的开发成本和复杂的部署模式。

## 各种shuffle方法

```python
def simple_shuffle(M, R, map, reduce):
    map_out = [map.remote(m) for m in range(M)]
    return ray.get([
        reduce.remote(map_out[:,r]) for r in range(R)])
def shuffle_riffle(M, R, F, map, reduce, merge):
    map_out = [map.remote(m) for m in range(M)]
    merge_out = [
        merge.remote(map_out[i*F:(i+1)*F, :])
        for i in range(M/F)]
    return ray.get([
        reduce.remote(merge_out[:,r]) for r in range(R)])
def shuffle_magnet(M, R, F, map, reduce, merge):
    map_out = [map.remote(m) for m in range(M)]
    merge_out = [
        [merge.remote(map_out[i*F:(i+1)*F, r])
        for i in range(M/F)] for r in range(R)]
    return ray.get(
        [reduce.remote(merge_out[:,r]) for r in range(R)])
```

M和R分别代表map和reduce任务的数量，合并因子表示将F个block合并为一个block。

simple_shuffle中reducer从mapper中拉取对应blocks，如果分区大小固定，shuffle的blocks数量随着数据总量增加呈二次方增长。

shuffle_riffle通过将小的map结果的block合并为一个更大的block，从而将reducer侧的小型、随机的磁盘I/O转为大型顺序的I/O。F个map结果的block合并为一个block，然后进行由reducer拉取。

Push-based Shuffle在计算完成后立即push结果块，而不是等待reducer拉取数据块。Magent通过后会合并中间结果数据块来减少磁盘I/O。和Riffle合并的不同点在于，Magent根据会根据reducer侧分区情况对map结果进行划分再合并中间结果。

```python
def push_based_shuffle(map, reduce):
    @ray.remote
    def merge(*map_results):
        # 转置， output[i, j] -> output[j, i]
        # 将map结果从按照mapper顺序收集转为按照reducer顺序收集
        for results in zip(*map_results):
            yield reduce(*results)
    
    merge_results = numpy.empty((NUM_WORKERS,
        NUM_ROUNDS, NUM_REDUCERS_PER_WORKER))
    # Map and shuffle.
    # 多个round pipeline执行
    for rnd in range(NUM_ROUNDS):
        for i in range(NUM_TASKS_PER_ROUND):
            map_results = [
                # 每个task产生集群中对应worker数量个输出结果
                map.options(num_returns=NUM_WORKERS).remote(
                    # 一次执行NUM_TASKS_PER_ROUND个task
                    parts[rnd * NUM_TASKS_PER_ROUND + i])
                for i in range(NUM_TASKS_PER_ROUND)]
        if rnd > 0:
            ray.wait(merge_results[:, rnd - 1, :])
        for w in range(NUM_WORKERS):
            merge_results[w, rnd, :] = merge.options(
                # 亲和性调度，将merge任务调度到对应的reducer worker上执行
                worker=w, num_returns=NUM_REDUCERS_PER_WORKER
            ).remote(*map_results[:, w])
        del map_results
    # Reduce.
    return flatten(
        [[reduce.remote(*merge_results[w, :, rnd])
            for rnd in range(NUM_REDUCERS_PER_WORKER)]
            for w in range(NUM_WORKERS)])
```

滞后任务、数据倾斜、pipeline

```python
class PushBasedShuffleTaskScheduler(ExchangeTaskScheduler):

    def execute(
        self,
        refs: List[RefBundle],
        output_num_blocks: int,
        task_ctx: TaskContext,
        map_ray_remote_args: Optional[Dict[str, Any]] = None,
        reduce_ray_remote_args: Optional[Dict[str, Any]] = None,
        merge_factor: float = 2,
    ) -> Tuple[List[RefBundle], StatsDict]:
        map_fn = self._map_partition
        merge_fn = self._merge

        def map_partition(*args, **kwargs):
            return map_fn(self._exchange_spec.map, *args, **kwargs)

        def merge(*args, **kwargs):
            return merge_fn(self._exchange_spec.reduce, *args, **kwargs)

        map_stage_iter = _MapStageIterator(
            input_blocks_list,
            shuffle_map,
            [output_num_blocks, stage.merge_schedule, *self._exchange_spec._map_args],
        )
        map_stage_executor = _PipelinedStageExecutor(
            map_stage_iter, stage.num_map_tasks_per_round, progress_bar=map_bar
        )
        
        shuffle_map = cached_remote_fn(map_partition)
        shuffle_map = shuffle_map.options(
            **map_ray_remote_args,
            num_returns=1 + stage.merge_schedule.num_merge_tasks_per_round,
        )

        shuffle_merge = cached_remote_fn(merge)
        merge_stage_iter = _MergeStageIterator(
            map_stage_iter, shuffle_merge, stage, self._exchange_spec._reduce_args
        )
        merge_stage_executor = _PipelinedStageExecutor(
            merge_stage_iter,
            stage.merge_schedule.num_merge_tasks_per_round,
            max_concurrent_rounds=2,
        )

        # 执行map-merge阶段，提交M个map任务和N个merge任务
        # map与merge之间的任务执行是pipeline，这一个round的merge任务执行时，下一个round的map任务可以开始执行。
        map_done = False
        merge_done = False
        map_stage_metadata_schema = []
        merge_stage_metadata_schema = []
        while not (map_done and merge_done):
            try:
                map_stage_metadata_schema += next(map_stage_executor)
            except StopIteration:
                map_done = True
                break

            try:
                merge_stage_metadata_schema += next(merge_stage_executor)
            except StopIteration:
                merge_done = True
                break

        all_merge_results = merge_stage_iter.pop_merge_results()

        # 执行reduce任务
        shuffle_reduce = cached_remote_fn(self._exchange_spec.reduce)
        reduce_stage_iter = _ReduceStageIterator(
            stage,
            shuffle_reduce,
            all_merge_results,
            reduce_ray_remote_args,
            self._exchange_spec._reduce_args,
            _debug_limit_execution_to_num_blocks,
        )

        max_reduce_tasks_in_flight = output_num_blocks

        reduce_stage_executor = _PipelinedStageExecutor(
            reduce_stage_iter,
            max_reduce_tasks_in_flight,
            max_concurrent_rounds=2,
            progress_bar=reduce_bar,
        )
        reduce_stage_metadata_schema = []
        while True:
            try:
                reduce_stage_metadata_schema += next(reduce_stage_executor)
            except StopIteration:
                break

            self.warn_on_high_local_memory_store_usage()

        output = []
        for block, meta_with_schema in zip(new_blocks, reduce_stage_metadata_schema):
            output.append(
                RefBundle(
                    [
                        (
                            block,
                            meta_with_schema.metadata,
                        )
                    ],
                    owns_blocks=input_owned,
                    schema=meta_with_schema.schema,
                )
            )

        return (output, stats)

    def _map_partition(
        map_fn,
        idx: int,
        block: Block,
        output_num_blocks: int,
        schedule: _MergeTaskSchedule,
        *map_args: List[Any],
    ) -> List[Union[Block, "BlockMetadataWithSchema"]]:
        mapper_outputs = map_fn(idx, block, output_num_blocks, *map_args)

        # A merge task may produce results for multiple downstream reducer
        # tasks. Therefore, each map task should give each merge task a
        # partition of its outputs, where the length of the partition is equal
        # to the number of reducers downstream to the merge task.
        partition = []
        merge_idx = 0
        while merge_idx < schedule.num_merge_tasks_per_round and mapper_outputs:
            output = mapper_outputs.pop(0)
            partition.append(output)

            # 达到num_reducers_per_merge数量个partition后输出到下游任务
            if len(partition) == schedule.get_num_reducers_per_merge_idx(merge_idx):
                yield partition

                partition = []
                merge_idx += 1

        yield mapper_outputs[0]

        assert merge_idx == schedule.num_merge_tasks_per_round, (
            merge_idx,
            schedule.num_merge_tasks_per_round,
        )

    def _merge(
        reduce_fn,
        *all_mapper_outputs: List[List[Block]],
        reduce_args: Optional[List[Any]] = None,
    ) -> List[Union["BlockMetadataWithSchema", Block]]:
        """
        Returns list of [BlockMetadata, O1, O2, O3, ...output_num_blocks].
        """
        # 将map结果block从M*R转为R*M
        for i, mapper_outputs in enumerate(zip(*all_mapper_outputs)):
            block_meta_with_schema: Tuple[Block, "BlockMetadataWithSchema"] = reduce_fn(
                *reduce_args, *mapper_outputs, partial_reduce=True
            )
            block, meta_with_schema = block_meta_with_schema
            yield block
            schemas.append(meta_with_schema.schema)

        meta_with_schema = BlockMetadataWithSchema(metadata=meta, schema=schema)
        yield meta_with_schema

# 按照round提交任务
class _PipelinedStageExecutor:
    def __init__(
        self,
        stage_iter,
        num_tasks_per_round: int,
        max_concurrent_rounds: int = 1,
    ):
        self._stage_iter = stage_iter
        self._num_tasks_per_round = num_tasks_per_round
        self._max_concurrent_rounds = max_concurrent_rounds
        self._rounds: List[List[ObjectRef]] = []
        self._submit_round()

    def __next__(self) -> List["BlockMetadataWithSchema"]:
        """
        Submit one round of tasks. If we already have the max concurrent rounds
        in flight, first wait for the oldest round of tasks to finish.
        """
        prev_metadata_and_schema = []
        if all(len(r) == 0 for r in self._rounds):
            raise StopIteration

        if len(self._rounds) >= self._max_concurrent_rounds:
            # 获取上一个round的task结果
            prev_metadata_schema_refs = self._rounds.pop(0)
            prev_metadata_and_schema = ray.get(prev_metadata_schema_refs)

        # 提交下一个round的task
        self._submit_round()

        return prev_metadata_and_schema

    def _submit_round(self):
        assert len(self._rounds) < self._max_concurrent_rounds
        task_round = []
        for _ in range(self._num_tasks_per_round):
            # 提交下一个round的task
            try:
                task_round.append(next(self._stage_iter))
            except StopIteration:
                break
        self._rounds.append(task_round)

class _MapStageIterator:

    def __next__(self):
        # 读取input，执行map任务
        if not self._input_blocks_list:
            raise StopIteration
        block = self._input_blocks_list.pop(0)
        map_result = self._shuffle_map.remote(
            self._mapper_idx,
            block,
            *self._map_args,
        )
        metadata_schema_ref = map_result.pop(-1)
        self._map_results.append(map_result)
        self._mapper_idx += 1
        return metadata_schema_ref

class _MergeStageIterator:

    def __next__(self):
        if not self._map_result_buffer or not self._map_result_buffer[0]:
            assert self._merge_idx == 0
            self._map_result_buffer = self._map_stage_iter.pop_map_results()

        if not self._map_result_buffer:
            raise StopIteration

        # Shuffle the map results for the merge tasks.
        merge_args = [map_result.pop(0) for map_result in self._map_result_buffer]
        num_merge_returns = self._stage.merge_schedule.get_num_reducers_per_merge_idx(
            self._merge_idx
        )
        merge_result = self._shuffle_merge.options(
            num_returns=1 + num_merge_returns,
            **self._stage.get_merge_task_options(self._merge_idx),
        ).remote(
            *merge_args,
            reduce_args=self._reduce_args,
        )
        metadata_schema_ref = merge_result.pop(-1)
        self._all_merge_results[self._merge_idx].append(merge_result)
        del merge_result

        self._merge_idx += 1
        self._merge_idx %= self._stage.merge_schedule.num_merge_tasks_per_round
        return metadata_schema_ref 

class _ReduceStageIterator:
    def __init__(
        self,
        stage: _PushBasedShuffleStage,
        shuffle_reduce,
        all_merge_results: List[List[List[ObjectRef]]],
        ray_remote_args,
        reduce_args: List[Any],
    ):
        for reduce_idx in self._stage.merge_schedule.round_robin_reduce_idx_iterator():
            merge_idx = self._stage.merge_schedule.get_merge_idx_for_reducer_idx(
                reduce_idx
            )
            # 设置reducer的对应输入
            reduce_arg_blocks = [
                merge_results.pop(0) for merge_results in all_merge_results[merge_idx]
            ]
            self._reduce_arg_blocks.append((reduce_idx, reduce_arg_blocks))

    def __next__(self):
        reduce_idx, reduce_arg_blocks = self._reduce_arg_blocks.pop(0)
        merge_idx = self._stage.merge_schedule.get_merge_idx_for_reducer_idx(reduce_idx)
        # Submit one partition of reduce tasks, one for each of the P
        # outputs produced by the corresponding merge task.
        # We also add the merge task arguments so that the reduce task
        # is colocated with its inputs.
        # reduce结果
        block, meta_with_schema = self._shuffle_reduce.options(
            **self._ray_remote_args,
            **self._stage.get_merge_task_options(merge_idx),
            num_returns=2,
        ).remote(*self._reduce_args, *reduce_arg_blocks, partial_reduce=False)
        self._reduce_results.append((reduce_idx, block))
        return meta_with_schema

    def _compute_shuffle_schedule(
        num_cpus_per_node_map: Dict[str, int],
        num_input_blocks: int,
        merge_factor: float,
        num_output_blocks: int,
    ) -> _PushBasedShuffleStage:
        # 控制一个merge对应的map数量
        num_tasks_per_map_merge_group = merge_factor + 1
        num_total_merge_tasks = math.ceil(num_input_blocks / merge_factor)
```

