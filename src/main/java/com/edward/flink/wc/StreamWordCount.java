package com.edward.flink.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount {

    public static void main(String[] args) throws Exception {


        //获取参数  参数格式 --host xx.xx.xx.xx --port  8888
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        Integer port = parameterTool.getInt("port");


        //创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读文本数据
        //DataStreamSource<String> lineDataStreamSource = env.readTextFile("input/words.txt");

        //读取端口数据
        //nc -lk 8888
        DataStreamSource<String> lineDataStreamSource = env.socketTextStream("192.168.56.10", 8888);

        //对数据进行拆分
        SingleOutputStreamOperator<Tuple2<String, Long>> tuple2SingleOutputStreamOperator = lineDataStreamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) ->
        {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        //对数据进行分组
        KeyedStream<Tuple2<String, Long>, String> tuple2StringKeyedStream = tuple2SingleOutputStreamOperator.keyBy(data -> data.f0);

        SingleOutputStreamOperator<Tuple2<String, Long>> sum = tuple2StringKeyedStream.sum(1);

        sum.print();

        env.execute();

    }
}
