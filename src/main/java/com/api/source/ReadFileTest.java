package com.api.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReadFileTest {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = environment.readTextFile("D:\\work\\flink-demo\\src\\main\\resources\\file.txt");

        dataStream.print();

        environment.execute();
    }
}
