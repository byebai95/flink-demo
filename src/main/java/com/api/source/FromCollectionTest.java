package com.api.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class FromCollectionTest {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String,Integer>> dataStream = environment.fromCollection(Arrays.asList(
                new Tuple2<>("Hello", 10),
                new Tuple2<>("World", 2)
        ));

        dataStream.print();
        environment.execute("Job01");

    }
}
