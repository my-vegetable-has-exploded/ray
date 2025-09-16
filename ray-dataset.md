## dataset

ray dataset主要执行入口包括count()、take()等方法，调用链路为`dataset.take() -> dataset.iter_rows() -> dataset._iter_batches() -> DataIteratorImpl._to_ref_bundle_iterator() -> dataset._execute_to_iterator -> ExecutorPlan.execute_to_iterator`。在`ExecutorPlan.execute_to_iterator`中使用`execute_to_legacy_bundle_iterator`方法中生成优化后的`PhysicalPlan`并创建`StreamingExecutor`以及返回`bundle_iter`。核心为调用`StreamingExecutor.execute`方法来进行实际执行。

```python
    def execute(
        self, dag: PhysicalOperator, initial_stats: Optional[DatasetStats] = None
    ) -> OutputIterator:
        if not isinstance(dag, InputDataBuffer):
		# 构建执行计划拓扑
        self._topology, _ = build_streaming_topology(dag, self._options)
		# 设置资源管理器
        self._resource_manager = ResourceManager(...)
		# 创建背压策略
        self._backpressure_policies = get_backpressure_policies(
            self._data_context, self._topology, self._resource_manager
        )
		# 创建autoscaler
        self._autoscaler = create_autoscaler(...)
		# 注册统计actor
        StatsManager.register_dataset_to_stats_actor(... )
        self.start()

        return _ClosingIterator(self)

    def run(self):
        exc: Optional[Exception] = None
        try:
            while True:
				# _scheduling_loop_step
                continue_sched = self._scheduling_loop_step(self._topology)
                for callback in get_execution_callbacks(self._data_context):
                    callback.on_execution_step(self)
                if not continue_sched or self._shutdown:
                    break
        except Exception as e:
            exc = e
        finally:
            state.mark_finished(exc)

    def _scheduling_loop_step(self, topology: Topology) -> bool:
		# process_completed_tasks等待活跃tasks完成，然后将结果推送到输出队列
        num_errored_blocks = process_completed_tasks(topology,...)
        while True:
			# 根据资源和背压策略选择运行算子
            op = select_operator_to_run(
                topology,
                self._resource_manager,
                self._backpressure_policies...)
			# 调用对应operator执行任务
            topology[op].dispatch_next_task()
			
			if op is None:
                break
        # Trigger autoscaling
        self._autoscaler.try_trigger_scaling()
        # Keep going until all operators run to completion
        return not all(op.completed() for op in topology)
```

具体执行计划的执行在对应operator的`dispatch_next_task`中，调用`_add_input_inner`方法执行。以`MapOperator`为例，`_add_input_inner`方法首先将输入的`RefBundle`添加到`_block_ref_bundler`中。`BlockRefBundler`负责将多个小的`RefBundle`合并成更大的bundle，以提高处理效率。当bundler中有足够的数据时，会调用`_add_bundled_input`方法。`_add_bundled_input`方法会将`_map_task`转为ray.remote task并提交。 `StreamingExecutor.process_complete_task`会调用wait()方法，等待task完成，执行任务完成后的callback，如更新metric。

```python
	def _add_bundled_input(self, bundle: RefBundle):
        # 生成remote task
		gen = self._map_task.options(**dynamic_ray_remote_args).remote(
			self._map_transformer_ref,
			data_context,
			ctx,
			*bundle.block_refs,
			**self.get_map_task_kwargs(),
		)
		# 加入活跃任务列表
		self._submit_data_task(gen, bundle)

 def _map_task(
    map_transformer: MapTransformer,
    data_context: DataContext,
    ctx: TaskContext,
    *blocks: Block,
    **kwargs: Dict[str, Any],
) -> Iterator[Union[Block, "BlockMetadataWithSchema"]]:
    with MemoryProfiler(data_context.memory_usage_poll_interval_s) as profiler:
		# 调用map operator中转换函数
        for b_out in map_transformer.apply_transform(iter(blocks), ctx):
			......
			# 返回结果，任务已经被封装为ray中ObjectRefGenerator，方便迭代
            yield b_out
            yield meta_with_schema
			......
    TaskContext.reset_current()

def process_completed_tasks(
    topology: Topology,
    backpressure_policies: List[BackpressurePolicy],
    max_errored_blocks: int,
) -> int:
    # ...
       # 1. 收集所有正在运行的任务
       # active_tasks 是一个字典, key 是任务的 ObjectRef (或 Waitable), 
       # value 是一个元组, 包含任务所属的 OpState 和任务本身。
       active_tasks: Dict[Waitable, Tuple[OpState, OpTask]] = {}
       for op, state in topology.items():
           for task in op.get_active_tasks():
               active_tasks[task.get_waitable()] = (state, task)
       # ... (处理反压策略)
       # 2. 等待任一任务完成
       num_errored_blocks = 0
       if active_tasks:
           # 使用 ray.wait() 等待 active_tasks 字典中任何一个 ObjectRef 准备就绪。
           # fetch_local=False 意味着我们只检查元数据, 不会立即下载数据。
           # timeout=0.1 避免长时间阻塞。
           ready, _ = ray.wait(
               list(active_tasks.keys()),
               num_returns=len(active_tasks),
               fetch_local=False,
               timeout=0.1,
           )
           # ... (按 operator 分组和排序 ready tasks)
           ready_tasks_by_op = defaultdict(list)
           for ref in ready:
               state, task = active_tasks[ref]
               ready_tasks_by_op[state].append(task)
           # ... (处理已完成的任务)
       # 3. 将完成的数据块放入输出队列
       # 遍历拓扑中的每个 operator。
       for op, op_state in topology.items():
           # op.has_next() 检查 operator 内部是否有已经完成并准备好输出的数据块。
           while op.has_next():
               # op.get_next() 从 operator 内部取出完成的 RefBundle。
               # op_state.add_output() 将这个 RefBundle 添加到该 operator 的输出队列中。
               op_state.add_output(op.get_next())
       return num_errored_blocks
```

## source operator

source operator是数据源。因为需要支持多种数据源，所以source在ray data中是一类较为特殊的operator。

planner._plan_recursively将logical operator转换为physical operator的过程中，使用planner._DEFAULT_PLAN_FNS的函数将各种不同的logical operator转换为对应的physical operator。

对于read_csv、read_parquet这些函数封装成的logical operator，通过plan_read_op函数转化为InputDataBuffer。plan_read_op函数会调用

```python
def plan_read_op(
    op: Read,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> PhysicalOperator:
    """将逻辑上的 Read 算子转换为物理执行计划。"""
    assert len(physical_children) == 0

    # 1. 定义如何生成输入的 ReadTask 引用
    def get_input_data(target_max_block_size) -> List[RefBundle]:
        parallelism = op.get_detected_parallelism()
        # 从 Datasource 获取所有读取任务
        read_tasks = op._datasource_or_legacy_reader.get_read_tasks(parallelism)
        
        ret = []
        for read_task in read_tasks:
            # 将 ReadTask 放入对象存储，以便传递给远程任务
            read_task_ref = ray.put(read_task)
            # 将任务引用和元数据包装成 RefBundle
            ref_bundle = RefBundle(
                (
                    (
                        read_task_ref,
                        _derive_metadata(read_task, read_task_ref),
                    ),
                ),
                owns_blocks=False,
            )
            ret.append(ref_bundle)
        return ret

    # 2. 创建一个 InputDataBuffer，它将在执行时调用 get_input_data
    inputs = InputDataBuffer(data_context, input_data_factory=get_input_data)

    # 3. 定义如何执行一个 ReadTask
    def do_read(blocks: Iterable[ReadTask], _: TaskContext) -> Iterable[Block]:
        # blocks 在这里实际上是 ReadTask 对象
        for read_task in blocks:
            # 执行 read_task() 会返回一个或多个数据块 (Block)
            yield from read_task()

    # 4. 创建一个 MapTransformer，封装了 do_read 逻辑
    transform_fns: List[MapTransformFn] = [
        BlockMapTransformFn(do_read),
        BuildOutputBlocksMapTransformFn.for_blocks(),
    ]
    map_transformer = MapTransformer(transform_fns)

    # 5. 创建 MapOperator，这是最终返回的物理算子
    # 它会从 `inputs` 获取 ReadTask，并应用 `map_transformer` 来执行读取
    return MapOperator.create(
        map_transformer,
        inputs......
    )
```

StreamExecutor通过get_next()获取完成的InputDataBuffer中完成RefBundle。