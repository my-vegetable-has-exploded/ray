raylet通过启动不同语言进程，来执行不同语言的任务
```cpp
// worker_pool.cc
// 为不同构建进程启动命令， state根据不同语言设置了worker_commands用作初始化命令
  auto [worker_command_args, env] =
      BuildProcessCommandArgs(language,
                              job_config,
                              worker_type,
                              job_id,
                              dynamic_options,
                              runtime_env_hash,
                              serialized_runtime_env_context,
                              state);

  auto start = std::chrono::high_resolution_clock::now();
  // Start a process and measure the startup time.
  Process proc = StartProcess(worker_command_args, env);
```

以cpp的worker为例，执行链路如下：
`TaskReceiver::task_handler_` -> `CoreWorker::executetask` -> `options_.task_execution_callback`

`ProcessHelper::RayStart`中对`options_.task_execution_callback`进行初始化，设置为`taskexecutor::executetask`，`ProcessHelper::getinstance().RayStart(taskexecutor::executetask);` 

`TaskExecutor::ExecuteTask`调用`TaskExecutor::GetExecuteResult`，获取可以需要执行的函数

```cpp
EntryFuntion entry_function;
if (actor_ptr == nullptr) {
	entry_function = FunctionHelper::GetInstance().GetExecutableFunctions(func_name);
} else {
	entry_function =
		FunctionHelper::GetInstance().GetExecutableMemberFunctions(func_name);
}
RAY_LOG(DEBUG) << "Get executable function " << func_name << " ok.";
auto result = entry_function(func_name, args_buffer, actor_ptr);
RAY_LOG(DEBUG) << "Execute function " << func_name << " ok.";
```
