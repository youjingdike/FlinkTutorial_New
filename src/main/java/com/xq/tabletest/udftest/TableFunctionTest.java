package com.xq.tabletest.udftest;

import com.xq.apitest.pojo.SensorReading;
import com.xq.tabletest.TableApiTest;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.net.URL;
import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class TableFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env, settings);

        URL resource = TableApiTest.class.getResource("/sensor.txt");
        String filePath = resource.getPath();
        DataStreamSource<String> inputStream = env.readTextFile(filePath);

        WatermarkStrategy<SensorReading> watermarkStrategy = WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofMillis(1000L))
                .withTimestampAssigner((SerializableTimestampAssigner<SensorReading>) (element, recordTimestamp) -> element.getTimestamp());

        // 先转换成样例类类型（简单转换操作）
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map((MapFunction<String, SensorReading>) value -> {
            String[] split = value.split(",");
            return new SensorReading(split[0].trim(), Long.parseLong(split[1].trim()), Double.parseDouble(split[2].trim()));
        })
        .assignTimestampsAndWatermarks(watermarkStrategy);

        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.STRING())
                .column("temperature", DataTypes.DOUBLE())
//                .column("timestamp", DataTypes.BIGINT())
//                .columnByExpression("ts", Expressions.callSql("TO_TIMESTAMP_LTZ(`timestamp`, 0)"))
                .columnByExpression("ts", "TO_TIMESTAMP_LTZ(`timestamp`, 0)")//事件时间，类是字段与sql关键字冲突，要加上`号
                .columnByExpression("ps", "PROCTIME()")//操作时间
                .watermark("ts", "SOURCE_WATERMARK()")
//                .watermark("ts", sourceWatermark())
                .build();
        Table sensorTable = tabEnv.fromDataStream(dataStream, schema);

        // 首先new一个UDF的实例,并在在环境中注册UDF
        tabEnv.createTemporarySystemFunction("split", new Split("_"));
        //1. table api
        Table resultTable = sensorTable
//                .joinLateral("split(id) as (word,length)")
                .joinLateral(call("split",$("id")).as("word","length"))
                .select($("id"),$("ts"),$("word"),$("length"));

        //2.sql
        tabEnv.createTemporaryView("sensor",sensorTable);
        Table sqlRes = tabEnv.sqlQuery("select id,ts, word, length " +
                "from sensor,lateral table(split(id)) as t(word,length) ");

//        tabEnv.toAppendStream(resultTable, Row.class).print("table");
        tabEnv.toDataStream(sqlRes, Row.class).print("sql");


        env.execute("test table func");
    }
}
