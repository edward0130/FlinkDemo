package com.edward.flink.datastream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformFilterDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> stream = env.fromElements(new Event("Tom", "./home", 1000L),
                new Event("Jim", "./fav", 2000L),
                new Event("Lily", "./prod", 3000L));

        //1.通过匿名内部类的方式加载
        SingleOutputStreamOperator<Event> filter = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.user.equals("Tom");
            }
        });

        //2.使用自定义类
        SingleOutputStreamOperator<Event> filter1 = stream.filter(new MyFilterFunction());

        //3.使用lambda表达式加载
        SingleOutputStreamOperator<Event> filter2 = stream.filter(data -> data.user.equals("Tom"));

        //filter.print();
        filter1.print();
        filter2.print();
        env.execute();

    }

    public static class MyFilterFunction implements FilterFunction<Event>{

        @Override
        public boolean filter(Event event) throws Exception {
            return event.user.equals("Tom");
        }
    }
}
