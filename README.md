# MIT6.824
基于MIT分布式课程6.824的lab

## MapReduce
### MapReduce是一种编程模型，用于大规模数据集（大于1TB）的并行运算。概念"Map（映射）"和"Reduce（归约）"，是它们的主要思想，都是从函数式编程语言里借来的，还有从矢量编程语言里借来的特性。它极大地方便了编程人员在不会分布式并行编程的情况下，将自己的程序运行在分布式系统上。  
## 本次所实现的MapReduce的工作原理
+ 一个任务调度者进程coordinator调度若干个任务执行者进程worker去执行map和reduce任务。整个任务一开始的时候是启动coordinator进程，coordinator进程将初始化存储工作进度信息的数据结构，启动容灾线程，启动rpc服务。然后启动若干个worker进程，worker进程通过rpc向coordinator进程获取任务。coordinator进程根据工作进度的情况给每个worker进程分配任务，然后修改工作进度信息。worker进程执行完被分配的任务后通过rpc告知coordinator进程，coordinator进程修改工作进度信息，worker进程随即又通过rpc向coordinator进程获取下一个任务。
+ 当所有的map任务执行完了之后coordinator进程再向worker进程分配reduce任务，map任务的工作结果以json文件的形式保存。
+ coordinator代码中涉及到读写工作进度信息的地方都用了锁，实现互斥。
+ 容灾线程不停地轮询工作进度信息中存放已分配但是还没有完成的map和reduce任务的最近一次分配时间，如果当前时间与最近一次分配时间之差大于5秒，那么判定该任务已经挂了，当有新的worker进程来获取任务时就把该任务重新分配给这个worker进程。
## 代码结构
+ main包下的mrcoordinator.go和mrwoker.go里面有coordinator和worker的main函数，test-mr.sh是测试脚本。mr包下存放coordinator，rpc和worker的主要代码，mrapps包下存放的是测试用的动态链接库。其中mr包下的代码是我写的，你也可以进行改写，其他的包都是MIT6.824已经提供了的。关于代码结构的详细信息请参见[6.824 Lab 1: MapReduce (mit.edu)](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)。
+ 在linux操作系统下通过执行test-mr.sh脚本即可测试。