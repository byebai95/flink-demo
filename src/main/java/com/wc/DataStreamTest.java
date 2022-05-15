package com.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *  DataSet 批处理
 */
public class DataStreamTest {
    public static void main(String[] args) throws Exception{

        //1.准备本地执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置分布式计算的并行度
        environment.setParallelism(2);

        //2.输入
        String inputPath = "D:\\work\\flink-demo\\src\\main\\resources\\word.txt";
        DataStreamSource<String> dataSource = environment.readTextFile(inputPath);

        DataStream<Tuple2<String, Integer>> result = dataSource.flatMap(new MyFlatMap())
                .keyBy("0")
                .sum(1);
        result.print();

        //3.启动任务
        environment.execute();

    }

    static class MyFlatMap implements FlatMapFunction<String,Tuple2<String,Integer>>{

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] wordList = s.split(" ");
            for(String str : wordList){
                collector.collect(new Tuple2<String,Integer>(str,1));
            }
        }
    }
}
