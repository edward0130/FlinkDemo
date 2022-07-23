package com.edward.flink.datastream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformFlatMapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<Event> stream = env.fromElements(new Event("Tom", "./home", 1000L),
                new Event("Jim", "./fav", 2000L),
                new Event("Lily", "./prod", 3000L));


        //1.通过匿名内部类的方式实现FlatMapFunction接口;
        SingleOutputStreamOperator<String> flatMapStream = stream.flatMap(new FlatMapFunction<Event, String>() {
            @Override
            public void flatMap(Event event, Collector<String> out) throws Exception {
                out.collect(event.user);
                out.collect(event.url);
                out.collect(event.timestamp.toString());
            }
        });

        //2.通过lambda

        SingleOutputStreamOperator<String> flatMapStream1 = stream.flatMap((Event value, Collector<String> out) -> {
                    out.collect(value.user);
                    out.collect(value.url);
                    out.collect(value.timestamp.toString());
                }
        ).returns(new TypeHint<String>() {
        });

        //3.通过自定义类
        SingleOutputStreamOperator<String> flatMapStream2 = stream.flatMap(new myFlatMapFunction());

        //flatMapStream.print();
        //flatMapStream1.print();
        flatMapStream2.print();



        env.execute();

    }

    public static class myFlatMapFunction implements FlatMapFunction<Event,String>{

        @Override
        public void flatMap(Event event, Collector<String> out) throws Exception {
            out.collect(event.user);
            out.collect(event.url);
            out.collect(event.timestamp.toString());
        }
    }
}
