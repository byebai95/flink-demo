package com.api.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaTest {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");

        DataStream<String> dataStream = environment.addSource(new FlinkKafkaConsumer("test",new SimpleStringSchema(),properties));
        dataStream.print();
        environment.execute();

    }
}
