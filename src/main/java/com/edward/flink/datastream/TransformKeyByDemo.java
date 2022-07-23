package com.edward.flink.datastream;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformKeyByDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(new Event("Tom", "./home", 1000L),
                new Event("Jim", "./fav", 2000L),
                new Event("Lily", "./prod", 3000L),
                new Event("Jim", "./home", 4000L),
                new Event("Lily", "./fav", 5000L));

        SingleOutputStreamOperator<Event> keyByStream = stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.user;
            }
        }).max("timestamp");

        //2.通过lambda表达式
        SingleOutputStreamOperator<Event> keyByStream1 = stream.keyBy(data->data.user)
                .maxBy("timestamp");

        keyByStream.print("1");
        keyByStream1.print("2");

        env.execute();


    }
}
