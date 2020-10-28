package com.xq.tabletest;

import com.xq.apitest.pojo.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.net.URL;

public class TableApiTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        /*URL resource = TableApiTest.class.getResource("/sensor.txt");
        String filePath = resource.getPath().toString();*/

        // 1.1 基于老版本planner的流处理
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useOldPlanner().build();
        StreamTableEnvironment oldStreamTableEnv = StreamTableEnvironment.create(env, settings);

        //1.2 基于新


//        tableEnv.execute("table api test");
    }
}
