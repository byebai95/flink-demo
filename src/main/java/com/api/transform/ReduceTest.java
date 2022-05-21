package com.api.transform;

import com.api.model.SensorDTO;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceTest {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStream<String> inputStream = environment.readTextFile("D:\\work\\flink-demo\\src\\main\\resources\\sensor.txt");

        //1.转换数据类型
        DataStream<SensorDTO> dataStream = inputStream.map(new MapFunction<String, SensorDTO>() {
            @Override
            public SensorDTO map(String s) throws Exception {
                String[] strArr = s.split(",");
                return new SensorDTO(strArr[0],strArr[1],Double.valueOf(strArr[2]));
            }
        });

        //2.分组
        KeyedStream<SensorDTO, Tuple> keyedStream = dataStream.keyBy("id");

        //3.输出分组后最大温度与最新时间戳

        DataStream<SensorDTO> result = keyedStream.reduce(new ReduceFunction<SensorDTO>() {
            @Override
            public SensorDTO reduce(SensorDTO source, SensorDTO latest) throws Exception {
                return new SensorDTO(source.getId(),latest.getTimestamp(),Math.max(source.getTemperature(),latest.getTemperature()));
            }
        });

        result.print();

        environment.execute();

    }
}














