package com.xq.tabletest;

import com.xq.apitest.pojo.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.net.URL;

import static org.apache.flink.table.api.Expressions.$;

public class Example {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        URL resource = Example.class.getResource("/sensor.txt");
        DataStreamSource<String> inputStream = env.readTextFile(resource.getPath().toString());

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map((MapFunction<String, SensorReading>) value -> {
            String[] split = value.split(",");
            return new SensorReading(split[0].trim(), Long.parseLong(split[1].trim()), Double.parseDouble(split[2].trim()));
        });

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table table = tableEnv.fromDataStream(dataStream);

        // 调用table api进行转换
        Table filter = table
//                .select("id,temperature")
//                .filter("id == 'sensor_1'");
                .select($("id"),$("temperature"))
                .filter($("id").isEqual("sensor_1"));
        filter.printSchema();
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(filter, Row.class);
        rowDataStream.print("table");

        tableEnv.createTemporaryView("t1",table);
        tableEnv.createTemporaryView("t2",dataStream);

        Table res1 = tableEnv.sqlQuery("select id,temperature from t1 where id = 'sensor_1'");
        Table res2 = tableEnv.sqlQuery("select id,temperature from t2 where id = 'sensor_1'");

        DataStream<Row> rowDataStream1 = tableEnv.toDataStream(res1, Row.class);
        DataStream<Row> rowDataStream2 = tableEnv.toDataStream(res2, Row.class);

        rowDataStream1.print("t1");
        rowDataStream2.print("t2");

        env.execute("table test");
    }
}
