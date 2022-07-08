## 程序启动



## 运行模式 Yarn
1. Session 模式
参数配置
>
设置环境变量
`HADOOP_HOME=/opt/hadoop/hadoop-3.3.0
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
export HADOOP_CLASSPATH=`hadoop classpath`
`
启动命令
>yarn-session.sh -d
>flink run -c com.edward.flink.wc.StreamWordCount -p 2 ../lib/FlinkDemo-1.0-SNAPSHOT.jar
>flink cancel jobID
>flink list //查询running job

2. Job 模式

>flink run -d -t yarn-per-job -c com.edward.flink.wc.StreamWordCount -p 2 ../lib/FlinkDemo-1.0-SNAPSHOT.jar

3. Application 模式
>flink run-application -d -t yarn-application -c com.edward.flink.wc.StreamWordCount -p 2 ../lib/FlinkDemo-1.0-SNAPSHOT.jar

date -s "20220708 15:48:00"
