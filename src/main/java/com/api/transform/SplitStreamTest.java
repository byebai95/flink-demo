package com.api.transform;

import com.api.model.SensorDTO;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

public class SplitStreamTest {
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

        //3.设置标签，后续select 可以选择多个标签
        SplitStream<SensorDTO> splitStream = keyedStream.split(new OutputSelector<SensorDTO>() {
            @Override
            public Iterable<String> select(SensorDTO sensorDTO) {
                return (sensorDTO.getTemperature() > 36) ? Collections.singletonList("high") : Collections.singletonList("low") ;
            }
        });
        DataStream<SensorDTO> highStream = splitStream.select("high");
        DataStream<SensorDTO> lowStream = splitStream.select("low");

        highStream.print("high");
        lowStream.print("low");

        //4.合并流 high .low ，第一步 高温流提示报警信息
        DataStream<Tuple2<String,Double>> warnStream = highStream.map(new MapFunction<SensorDTO, Tuple2<String,Double>>() {
            @Override
            public Tuple2<String,Double> map(SensorDTO sensorDTO) throws Exception {
                return new Tuple2<String,Double>(sensorDTO.getId(),sensorDTO.getTemperature());
            }
        });


        //5.合并高温流与低温流
        ConnectedStreams<Tuple2<String,Double>, SensorDTO> connectedStreams = warnStream.connect(lowStream);

        //6.处理独自的流并进行合并
        DataStream<Object> resultStream = connectedStreams.map(new CoMapFunction<Tuple2<String,Double>, SensorDTO, Object>() {
            @Override
            public Object map1(Tuple2<String,Double> tuple2) throws Exception {
                return new Tuple3<>(tuple2.f0,tuple2.f1,"high temperature warning");
            }

            @Override
            public Object map2(SensorDTO sensorDTO) throws Exception {
                return new Tuple2<>(sensorDTO.getId(),"normal");
            }
        });

        //7.输出
        resultStream.print("connect");

        environment.execute();
    }
}
