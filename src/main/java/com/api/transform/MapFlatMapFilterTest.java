package com.api.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class MapFlatMapFilterTest {
    public static void main(String[] args) throws Exception{


        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStream<String> inputStream = environment.readTextFile("D:\\work\\flink-demo\\src\\main\\resources\\file.txt");

        //1. flatMap 将数据打散
        DataStream<String> flatStream = inputStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String str, Collector<String> collector) throws Exception {
                String[] stringArr = str.split(",");
                for(String s : stringArr){
                    collector.collect(s);
                }
            }
        });
        flatStream.print("FlatMap");

        //2. Map 操作，返回字符串长度
        DataStream<Integer> mapStream = inputStream.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return s.length();
            }
        });
        mapStream.print("Map");

        //3.Filter 过滤
        DataStream<String> filterStream = inputStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.startsWith("read_");
            }
        });
        filterStream.print("Filter");


        environment.execute();
    }
}
