# Flow Insight

## 设计与实现

https://mp.weixin.qq.com/s/KehYVdkdEC-9H7jaDMIlow

flow insight主要提供了对分布式系统任务进行监控分析，包括复杂的调用关系、性能瓶颈、资源利用。当前相关内容主要针对python语言部分的信息进行收集，没有对java/c++进行支持。

### Logical View

logical view提供了逻辑调用和任务依赖关系，类似于查询引擎的执行计划。

logical view的实现通过对python sdk的相关调用函数进行插桩进行实现。例如： 对remote function调用过程中参数解析进行记录，可以得到caller、callee信息以及二者之间对数据的依赖关系。

```python
cdef prepare_args_internal(...)
......
	            # 记录remote function的参数
                record_object_arg_put(put_id.Hex().decode(), i, size, function_descriptor.repr)
......

def record_object_arg_put(object_id, argpos, size, callee):
    """
    record the object put event for the task's args
    this will get caller context automatically from the runtime context
    callee is used to prevent recursive call for monitor actor

    param:
        object_id: the object id of the task's args
        size: the size of the task's args
        callee: the callee function info, e.g. "ActorClass.method_name"
    """
        caller_class = _get_caller_class()
        caller_func = _get_current_task_name()
        # Create a record for this call
        job_id = get_current_job_id()

        get_insight_client().emit_event(
            ObjectPutEvent(
                flow_id=job_id,
                object_id=object_id,
                object_size=size,
                object_pos=argpos,
                sender_service=caller_class[0],
                sender_instance_id=caller_class[1],
                sender_method=caller_func,
                timestamp=int(time.time() * 1000),
            )
        )
```

### Physical View

Physical view主要展示actor/task在集群节点上的放置情况，以及各节点的资源利用情况信息。

```python
class InsightHead(dashboard_utils.DashboardHeadModule):
    async def _emit_node_physical_stats(self):
        for actor_id, actor_info in DataSource.actors.items():
            flow_id = actor_info.get("jobId")

			# 获取节点位置信息
            node_id = actor_info["address"]["rayletId"]
            if node_id in DataSource.node_physical_stats:
                if actor_pid and "workers" in DataSource.node_physical_stats[node_id]:
                    for worker in node_physical_stats["workers"]:
                        if worker.get("pid") == actor_pid:
                            service_stats.cpu_percent = worker.get(
                                "cpuPercent", 0)
                            memory_info = worker.get(
                                "memoryInfo",
                                ......)
                            service_stats.memory_info = MemoryInfo(
                                rss=memory_info["rss"],
                                vms=memory_info["vms"],
                                shared=memory_info["shared"],
                                text=memory_info["text"],
                                lib=memory_info["lib"],
                                data=memory_info["data"],
                                dirty=memory_info["dirty"],
                            )
                            break
```

Physcial中节点数据来源于dashboard资源监控模块，actor等资源使用数据则是基于申请的资源数量。

### Call Stack

call stack主要在logical view上基础上记录了任务的调用次数，在CallSubmitEvent上报时进行数据聚合得到结果。

```python
# insight.py
def record_control_flow(callee_class, callee_func):
    """
    record the control flow between the caller and the callee
    this will get caller context automatically from the runtime context

    param:
        callee_class: the class name of the callee
        callee_func: the function name of the callee
    """

        caller_class = _get_caller_class()
        caller_func = _get_current_task_name()
        current_task_id = get_current_task_id()

        # Create a record for this call
        job_id = get_current_job_id()

        get_insight_client().emit_event(
            CallSubmitEvent(
                flow_id=job_id,
                source_service=caller_class[0],
                source_instance_id=caller_class[1],
                source_method=caller_func,
                target_service=None if callee_class is None else callee_class[0],
                target_instance_id=None if callee_class is None else callee_class[1],
                target_method=callee_func,
                timestamp=int(time.time() * 1000),
                parent_span_id=current_task_id,
            )
        )

# flow insight , js module
await snapshot.update_call_graph(
    flow_id,
    source_service,
    source_method,
    target_service,
    target_method,
    lambda record: {"count": record.get("count", 0) + 1, "start_time": start_time},
)
```

### Flame Graph

flame graph主要通过对task执行的时间进行记录实现。

```python
cdef CRayStatus task_execution_handler(......) nogil:

        # directly use record_task_duration here
        # rather than timeit to prevent indent change
        # to avoid diff conflict when merge new features
		# 记录开始时间
        start_time = time.time()
        try:
            try:
                execute_task_with_cancellation_handler(
        )
        finally:
            if start_time is not None:
				# 记录执行时间差
                record_task_duration(time.time() - start_time)

def record_task_duration(duration):
    try:
        caller_class = _get_caller_class()
        caller_func = _get_current_task_name()
        current_task_id = get_current_task_id()
        job_id = get_current_job_id()
        get_insight_client().emit_event(
            CallEndEvent(
                flow_id=job_id,
                target_service=caller_class[0],
                target_instance_id=caller_class[1],
                target_method=caller_func,
                duration=duration,
                span_id=current_task_id,
                timestamp=int(time.time() * 1000),
            )
        )
```

### Gantt

通过上述记录中的时间戳，绘制task的Gantt图。

### 界面支持

_ray_internal_insight_monitor这个actor负责接收各种相关Event上报，_ray_internal_insight_monitor	内部会启动一个http服务，供前端使用相关数据。

前端被封装为一个独立的"@ant-ray/flow-insight"包，项目中引入其作为前端使用。

## 编译

参考dashboard的编译步骤，flow insight编译需要安装更高版本的node，node@14版本编译会报错，测试高版本的node@20编译成功。

## 运行

dashboard使用flow insight时需要设置环境变量:

```bash
export RAY_FLOW_INSIGHT=1
ray start --head --port=6379 --include-dashboard=true --dashboard-host=0.0.0.0 --dashboard-port=8265
```