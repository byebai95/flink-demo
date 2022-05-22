package com.api.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class TableFileSourceTest {
    public static void main(String[] args) throws Exception{

        //1.配置环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.配置输入
        String path = "D:\\work\\flink-demo\\src\\main\\resources\\sensor.txt";
        tableEnv.connect(new FileSystem().path(path))
                .withFormat(new Csv())
                .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("timestamp",DataTypes.STRING())
                .field("temperature",DataTypes.DOUBLE()))
                .createTemporaryTable("inputPath");

        //3.输出表结构，输出数据
        Table table = tableEnv.from("inputPath");
//        table.printSchema();
//        tableEnv.toAppendStream(table, Row.class).print();

        //4.简单查询
        Table resultTable = table.select("id,temperature").filter("id = '1'");

        //5.聚合查询
        Table aggTable = table.groupBy("id")
                .select("id,id.count,temperature.avg")
                .filter("id = '1'");

        //6.sql 聚合查询
        Table sqlAggTable = tableEnv.sqlQuery("select id,count(id) as cnt,avg(temperature) avgTemp from inputPath group by id");


        //7.输出
        tableEnv.toAppendStream(resultTable, Row.class).print("resultTable");
        tableEnv.toRetractStream(aggTable, Row.class).print("aggTable");
        tableEnv.toRetractStream(sqlAggTable, Row.class).print("sqlAggTable");
        env.execute();
    }
}
