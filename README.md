# 本地环境提交flink on yarn作业

在使用云厂商提供的flink job管理平台时，通过界面操作提交flink任务到yarn上十分方便，那么开发调试时能否在本地环境直接提交flink任务到yarn呢？

开源的flink管理平台 [streampark](https://github.com/apache/incubator-streampark) 有提交flink on yarn作业的代码实现，可以参照 streampark 里对应模块的代码实现本地环境下的flink on yarn作业的提交。

其中 streampark-flink-client-core 作为提交flink job的核心模块，这里我们只关心flink on yarn作业的提交。

## flink job提交流程分析：
    详见博客文章：https://blog.csdn.net/Lee_3S/article/details/144128314


## java实现flink on yarn作业的提交

### 实现思路：

由上分析可知，提交flink job需要flink配置文件、hadoop环境变量，在本地环境下需要在项目中添加 flink-conf.yaml 配置文件，没有配置hadoop环境变量的话，可以自行添加 core-site.xml、hdfs-site.xml、yarn-site.xml 配置文件到项目指定路径中并创建YarnClient对象，或手动配置参数创建YarnClient对象。

剩下的就是将 streampark 的streampark-flink-client-core模块下的flink on yarn提交任务代码提取出来，通过阅读代码发现提交flink任务还需要`flink-dist_*.jar`文件，这是flink任务提交到yarn的前提条件之一。

### 实现流程：

创建自定义的任务提交客户端，通过HdfsUtils将任务jar包及依赖lib/路径上传至指定hdfs文件目录中，调用提交客户端的 doSubmit方法 提交任务到yarn集群，doCancel方法 取消正在运行的flink任务。

## Windows系统提交任务失败的问题解决

由于windows系统和linux系统下是不同的路径分隔符，导致windows下的本地环境提交flink on yarn作业失败（windows下找不到主类：YarnJobClusterEntrypoint）。

需在项目下创建 org.apache.flink.yarn 包路径，复制并修改 Utils 和 YarnClusterDescriptor 类文件，在启动时覆盖源码类加载执行，可以解决windows下提交任务失败的问题。

![](https://github.moeyy.xyz/https://raw.githubusercontent.com/Cyanty/images/main/collect/Snipaste_2024-11-22_11-13-37.png)

————————————————

[[FLINK-17858\] Yarn mode, windows and linux environment should be interlinked - ASF JIRA](https://issues.apache.org/jira/browse/FLINK-17858)




