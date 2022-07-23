package com.edward.flink.datastream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformMapDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(new Event("Lily", "./home", 1000L),
                new Event("Tom", "./fav", 2000L));

        //1.自定义类
        MyMapFunction myMapFunction = new MyMapFunction();
        SingleOutputStreamOperator<String> map = stream.map(myMapFunction);
        //2.匿名内部类
        SingleOutputStreamOperator<Object> map1 = stream.map(new MapFunction<Event, Object>() {
            @Override
            public Object map(Event event) throws Exception {
                return event.user;
            }
        });
        //3.lambda表达式
        SingleOutputStreamOperator<String> map2 = stream.map(data -> data.user);

        map.print();
        map1.print();
        map2.print();

        env.execute();
    }
    public static class MyMapFunction implements MapFunction<Event, String> {

        @Override
        public String map(Event event) throws Exception {
            return event.user;
        }
    }
}
