package com.api.table;

import com.api.model.SensorDTO;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TableApiTest {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("D:\\work\\flink-demo\\src\\main\\resources\\sensor.txt");
        DataStream<SensorDTO> dataStream = inputStream.map(line ->{
            String[] strArray = line.split(",");
            return new SensorDTO(strArray[0],strArray[1],Double.valueOf(strArray[2]));
        });

        StreamTableEnvironment tableEnv = StreamTableEnvironment .create(env);

        //1.使用 Table Api 查询
        Table table = tableEnv.fromDataStream(dataStream);
        Table tableResult = table.select("id,timestamp")
                .where("id = '1'");
        tableEnv.toAppendStream(tableResult, Row.class).print("table");


        //2.使用 SQL 查询 ,字段与表必须加上 ``
        tableEnv.createTemporaryView("sensor",dataStream);
        String sql = "select `id`,`timestamp` from `sensor` where id = '1'";
        Table sqlResult = tableEnv.sqlQuery(sql);
        tableEnv.toAppendStream(sqlResult, Row.class).print("sql");

        env.execute();
    }
}
