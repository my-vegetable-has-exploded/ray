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
	......
    # 处理完成的Ray任务并通知operators
    if active_tasks:
        ready, _ = ray.wait(
            list(active_tasks.keys()),
            num_returns=len(active_tasks),
            fetch_local=False,
            timeout=0.1,
        )

	# 将还有输出的operator的输出拉入到operator的state中
    for op, op_state in topology.items():
        while op.has_next():
            op_state.add_output(op.get_next())
    return num_errored_blocks
```