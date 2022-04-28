package com.xq.tabletest.udftest;

import com.xq.apitest.pojo.SensorReading;
import com.xq.tabletest.TableApiTest;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.net.URL;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.sourceWatermark;

public class AggregateFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env, settings);

        URL resource = TableApiTest.class.getResource("/sensor.txt");
        String filePath = resource.getPath();
        DataStreamSource<String> inputStream = env.readTextFile(filePath);

        // 先转换成样例类类型（简单转换操作）
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map((MapFunction<String, SensorReading>) value -> {
            String[] split = value.split(",");
            return new SensorReading(split[0].trim(), Long.parseLong(split[1].trim()), Double.parseDouble(split[2].trim()));
        })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.milliseconds(1000L)) {
                    @Override
                    public long extractTimestamp(SensorReading element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

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
//        Table sensorTable = tabEnv.fromDataStream(dataStream, $("id"),$("temperature"),$("timestamp").rowtime().as("ts"));
        Table sensorTable = tabEnv.fromDataStream(dataStream, schema);

//        tabEnv.registerFunction("avgTmp",new AvgTemp());
        tabEnv.createTemporarySystemFunction("avgTmp",new AvgTemp());
        Table select = sensorTable.groupBy($("id"))
//                .aggregate("avgTmp(temperature) as avgTemp")
                .aggregate(call("avgTmp", $("temperature")).as("avgTemp"))
                .select($("id"),$("avgTemp"));
//        tabEnv.toRetractStream(select, Row.class).print("tst");
        tabEnv.toChangelogStream(select).print("tst");

        env.execute("agg func");
    }
}
