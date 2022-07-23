package com.edward.flink.datastream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformReduceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(new Event("Tom", "./home", 1000L),
                new Event("Jim", "./fav", 2000L),
                new Event("Lily", "./prod", 3000L),
                new Event("Lily", "./fav", 4000L),
                new Event("Jim", "./home", 5000L),
                new Event("Lily", "./fav", 6000L),
                new Event("Rob", "./fav", 7000L));

        stream.map(data-> new Tuple2(data.user, 1L)).returns(Types.TUPLE(Types.STRING,Types.LONG))
                //把user当做key进行分组
                .keyBy(data->data.f0)
                //按key进行累加
                .reduce((data1,data2)->new Tuple2<String, Long>((String)data1.f0, (Long)data1.f1+(Long)data2.f1))
                //把所有数据放到一个分组里进行计算
                .keyBy(data-> "key")
                //获取当前访问量最大的用户
                .reduce((data1,data2)-> {
                    return (Long)data1.f1> (Long)data2.f1 ? data1 : data2;
                    })
                .print();

        env.execute();
    }
}
