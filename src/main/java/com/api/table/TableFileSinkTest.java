package com.api.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

public class TableFileSinkTest {
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
                .createTemporaryTable("inputTable");

        //3.输出表结构，输出数据
        Table table = tableEnv.from("inputTable");

        //4.简单查询
        Table resultTable = table.select("id,temperature").filter("id = '1'");

        //5.定义输出
        String outPath = "D:\\work\\flink-demo\\src\\main\\resources\\out.txt";
        tableEnv.connect(new FileSystem().path(outPath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temperature",DataTypes.DOUBLE()))
                .createTemporaryTable("outTable");
        tableEnv.insertInto("outTable",resultTable);

        env.execute();

    }
}
