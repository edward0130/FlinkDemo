package com.edward.flink.datastream;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

public class SinkToFileDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(4);

        DataStreamSource<Event> stream = env.fromElements(new Event("Tom", "./home", 1000L),
                new Event("Jim", "./fav", 2000L),
                new Event("Lily", "./prod", 3000L),
                new Event("Lily", "./fav", 4000L),
                new Event("Jim", "./home", 5000L),
                new Event("Lily", "./fav", 6000L),
                new Event("Rob", "./fav", 7000L));



        StreamingFileSink<String> rowFormatBuilder = StreamingFileSink.<String>forRowFormat(new Path("./output"),
                        new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder().
                        withMaxPartSize(1024 * 1024 * 1024) //1G一个文件
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))//15分钟一个文件
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))//没有数据写入间隔5分钟，生产新文件
                        .build())
                .build();
        // 通过addSink将数据写入文件
        stream.map(data -> data.toString()).addSink(rowFormatBuilder);

        env.execute();

    }
}

