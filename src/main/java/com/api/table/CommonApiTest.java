package com.api.table;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class CommonApiTest {
    public static void main(String[] args) {

        //1.基于老版本流处理
        EnvironmentSettings oldStreamSetting = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useOldPlanner()
                .build();
        StreamExecutionEnvironment oldEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(oldEnv,oldStreamSetting);

        //2.基于老版本批处理
        ExecutionEnvironment oldBatchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment oldBatchTableEnv = BatchTableEnvironment.create(oldBatchEnv);

        //3.基于 blink 流处理
        EnvironmentSettings blinkStreamSetting = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamExecutionEnvironment blinkEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment blinkStreamEnv = StreamTableEnvironment.create(blinkEnv,blinkStreamSetting);

        //4.基于 blink 批处理
        EnvironmentSettings blinkBatchSetting = EnvironmentSettings.newInstance()
                .inBatchMode()
                .useBlinkPlanner()
                .build();
        TableEnvironment blinkBatchTableEnv = TableEnvironment.create(blinkBatchSetting);

    }
}
