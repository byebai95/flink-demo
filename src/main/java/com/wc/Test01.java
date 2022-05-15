package com.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 *  DataSet 批处理
 */
public class Test01 {
    public static void main(String[] args) throws Exception{

        //1.准备本地执行环境
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        //2.输入
        String inputPath = "D:\\work\\flink-demo\\src\\main\\resources\\word.txt";
        DataSource<String> dataSource = environment.readTextFile(inputPath);

        DataSet<Tuple2<String, Integer>> result = dataSource.flatMap(new MyFlatMap())
                .groupBy("0")
                .sum(1);
        result.print();


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
