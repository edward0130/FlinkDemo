## 程序启动



## 运行模式 Yarn
1. Session 模式

1.1 参数配置
>设置环境变量
HADOOP_HOME=/opt/hadoop/hadoop-3.3.0
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
export HADOOP_CLASSPATH=\`hadoop classpath\`

启动命令
>yarn-session.sh -d
>flink run -c com.edward.flink.wc.StreamWordCount -p 2 ../lib/FlinkDemo-1.0-SNAPSHOT.jar
>flink cancel jobID
>flink list //查询running job

2. Job 模式

>flink run -d -t yarn-per-job -c com.edward.flink.wc.StreamWordCount -p 2 ../lib/FlinkDemo-1.0-SNAPSHOT.jar

3. Application 模式
>flink run-application -d -t yarn-application -c com.edward.flink.wc.StreamWordCount -p 2 ../lib/FlinkDemo-1.0-SNAPSHOT.jar

参数
>./bin/flink run-application -t yarn-application \
-Djobmanager.memory.process.size=1024m  \
-Dtaskmanager.memory.process.size=1024m  \
-Dyarn.application.name="Test" \
-Dtaskmanager.numberOfTaskSlots=3 \
-Dparallelism.default=3 \
-Dyarn.provided.lib.dirs="hdfs://myhdfs/remote-flink-dist-dir" \
hdfs://myhdfs/jars/MyApplication.jar

date -s "20220708 15:48:00"

4. 算子链
>disableChaining() --禁用当前算子的算子链
>startNewChain()--从当前算子开始新链
> env.disableOperatorChaining() --禁用全部算子链

5. 设置slot
>设置slot数量通过taskmanager.numberOfTaskSlots
>设置slot共享组 slotSharingGroup("g1")