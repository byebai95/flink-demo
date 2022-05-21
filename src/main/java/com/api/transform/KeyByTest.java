package com.api.transform;

import com.api.model.SensorDTO;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByTest {
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

        //滚动聚合
        //2.分组方式一 ,SensorDTO 需要提供无参构造
        KeyedStream<SensorDTO, Tuple> keyedStream = dataStream.keyBy("id");

        //分组方式二
        KeyedStream<SensorDTO, String> keyedStream1 = dataStream.keyBy(SensorDTO::getId);

        //max 只对局部字段更新, maxBy 会将整条数据更新
        //DataStream<SensorDTO> result = keyedStream1.max("temperature");
        DataStream<SensorDTO> result = keyedStream1.maxBy("temperature");

        result.print();
        environment.execute();


    }
}
