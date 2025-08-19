目标：希望能够利用HistoryServer，通过dashboard查询已退出的集群运行信息。

实现HistoryServer的步骤包括：1、持久化Dashboard数据到持久化存储中，在集群退出后，依然能否访问到历史数据信息；2、用户需要查看历史Dashboard数据时，从持久化存储正确地回复数据。

相关issue：https://github.com/ray-project/ray/issues/46444、https://github.com/ray-project/kuberay/issues/3884

## Dashboard当前数据采集存储方式

ray的dashboard中包含多个模块，包括节点监控（Nodes）、 作业管理（Jobs）、任务监控（Tasks）、事件监控（Events）、日志查看（Logs）。这些模块可能采用不同的数据采集和存储方式。

Ray中当前数据采集存储方式有下面几种方式：

### agent上报，head存储在内存中存储数据

https://github.com/ray-project/enhancements/pull/55/files

以event数据为例，ray中对这部分上报数据统一了export api（这些api在coreworker中，为不同语言提供统一的接口），ray任务运行先将这些任务数据以一定格式（如json）持久化到本地，然后本地agent监听文件，将文件内容上报给dashboard模块中的head进程，head进程将这些存放在内存字典中。

reporter模块中metrics数据采用类似方式，这些数据的agent在工作节点上不断执行资源监控脚本，使用top、nvidia-smi等命令获取资源使用情况，然后将这些数据上报给dashboard模块中的head进程，head进程同样将这些数据存放在内存字典中。

一些lib在任务执行过程中，主动代码上报数据，如data模块中，在dataset启动、数据bundle执行过程中，会主动上报数据给head进程。

### agent响应head请求，返回数据

log数据存储在各节点磁盘上，当用户在dashboard中查看日志时，head进程会向对应节点的log agent发送请求，返回数据。

job模块还承担了作业提交、状态查询等功能，job head向job agent发送作业请求，job agent响应请求，上报作业状态等信息，job的部分数据如job_info会放在internalkv中。

### Dashboard持久化方案

在集群退出之前，需要将dashboard的数据持久化，可以将数据放置在PVC或者消息队列中。

但是目前没有一个统一的api导出需要的数据。并且需要导出的数据格式、存储方式都不统一。

导出方式也需要考虑：1、在数据上报时双写一份到持久化存储中，还是2、在集群退出时统一导出。以方案1导出可以复用部分日志和export api的逻辑。 方案2修改会更统一一些。