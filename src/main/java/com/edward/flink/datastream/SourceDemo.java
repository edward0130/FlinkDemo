package com.edward.flink.datastream;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

public class SourceDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //1.从文件中读取数据
        DataStreamSource<String> stream1 = env.readTextFile("input/words.txt");

        //2.从集合中获取数据
        ArrayList<String> arrayList= new ArrayList<String>();
        arrayList.add("hello");
        arrayList.add("world");
        DataStreamSource<String> stream2 = env.fromCollection(arrayList);

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Bob","./index", 1000L));
        events.add(new Event("Lily","./he",2000L));
        DataStreamSource<Event> stream3 = env.fromCollection(events);

        //3.从元素读取元素
        DataStreamSource<Event> stream4 = env.fromElements(new Event("Bob", "./index", 1000L),
                new Event("Lily", "./he", 2000L));


        //4.从socket读取数据
//        DataStreamSource<String> stream5 = env.socketTextStream("node1", 8888);

        //5.从kafka读取数据

        //启动zookeeper ./bin/zookeeper-server-start.sh -daemon ./config/zookeeper.properties
        //ssh hadoop@node1 /opt/hadoop/apache-zookeeper-3.5.9-bin/bin/zkServer.sh start
        //ssh hadoop@node2 /opt/hadoop/apache-zookeeper-3.5.9-bin/bin/zkServer.sh start
        //ssh hadoop@node3 /opt/hadoop/apache-zookeeper-3.5.9-bin/bin/zkServer.sh start
        //启动kafka ./bin/kafka-server-start.sh -daemon ./config/server.properties

        //创建producer ./bin/kafka-console-producer.sh --broker-list node1:9092 --topic clicks
        //查询topic  ./bin/kafka-topics.sh --list --bootstrap-server 192.168.56.10:9092
        //查询topic内容 ./bin/kafka-console-consumer.sh --bootstrap-server node1:9092 --from-beginning --topic clicks
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node1:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset","latest");
        DataStreamSource<String> stream6 = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));

        //6.从自定义datasource获取数据

        DataStreamSource stream7 = env.addSource(new CustomDataSource());


//        stream1.print("1");
//        stream2.print("2");
//        stream3.print("3");
//        stream4.print("4");
//        stream5.print("5");
        //stream6.print("6");
        stream7.print("7");


        env.execute();
    }
}
