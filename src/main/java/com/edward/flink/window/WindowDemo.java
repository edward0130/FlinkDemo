package com.edward.flink.window;

import com.edward.flink.datastream.Event;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.IterableUtils;


public class WindowDemo {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<String> socketStream = env.socketTextStream("node1", 7777);


        socketStream.flatMap(new FlatMapFunction<String, Event>() {
            @Override
            public void flatMap(String s, Collector<Event> collector) throws Exception {
                String[] split = s.split(",");
                collector.collect(new Event(split[0],split[1], Long.valueOf(split[2])));
            }
        }).keyBy(data->data.user)

                //1.滚动窗口  timeWindow(Time.seconds(10)) 废弃,新版本默认使用事件事件
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))

                //2.滑动窗口  timeWindow(Time.seconds(10),Time.seconds(2)) 废弃
                //.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2)))

                //3.聚合操作  增量聚合
//                .aggregate(new AggregateFunction<Event, Integer, Integer>() {
//            @Override
//            public Integer createAccumulator() {
//                return 0;
//            }
//
//            @Override
//            public Integer add(Event event, Integer integer) {
//                return integer+1;
//            }
//
//            @Override
//            public Integer getResult(Integer integer) {
//                return integer;
//            }
//
//            @Override
//            public Integer merge(Integer integer, Integer acc1) {
//                return integer + acc1;
//            }
//        })
            //4.全量聚合   获取全量数据，然后进行聚合操作; WindowFunction ProcessWindowFunction
                .apply(new WindowFunction<Event, Tuple3<String, Long, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<Event> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                        String id = s;
                        Integer count = IteratorUtils.toList(input.iterator()).size();
                        Long end = window.getEnd();
                        out.collect(new Tuple3<>(id, end, count));
                    }
                })
            .print();

        env.execute();
    }
}
