package com.edward.flink.datastream;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class TransformPartitionDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(2);

        DataStreamSource<Event> stream = env.fromElements(new Event("Tom", "./home", 1000L),
                new Event("Jim", "./fav", 2000L),
                new Event("Lily", "./prod", 3000L),
                new Event("Lily", "./fav", 4000L),
                new Event("Jim", "./home", 5000L),
                new Event("Lily", "./fav", 6000L),
                new Event("Rob", "./fav", 7000L));

        //1.随机分区
        //stream.shuffle().print().setParallelism(4);

        //2.轮询分区
        //stream.rebalance().print().setParallelism(4);

        //3.rescale分区 根据不同节点进行就近重分区
//        env.addSource(new RichParallelSourceFunction<Integer>() {
//            @Override
//            public void run(SourceContext<Integer> ctx) throws Exception {
//                for (int i = 1; i <= 8; i++) {
//                    if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
//                        ctx.collect(i);
//                    }
//
//                }
//            }
//
//            @Override
//            public void cancel() {
//
//            }
//        }).setParallelism(2).rescale().print().setParallelism(4);

        //4.广播数据   数据同步到每个分区
        //stream.broadcast().print().setParallelism(4);

        //5.全局分区   数据只同步到一个分区
        //stream.global().print().setParallelism(4);

        //6.自定义分区
        stream.partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer integer, int i) {
                return integer%i;
            }
        }, new KeySelector<Event, Integer>() {
            @Override
            public Integer getKey(Event event) throws Exception {
                return event.user.length();
            }
        }).print();

        env.execute();
    }
}
