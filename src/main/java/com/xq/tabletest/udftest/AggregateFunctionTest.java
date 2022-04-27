package com.xq.tabletest.udftest;

import com.xq.apitest.pojo.SensorReading;
import com.xq.tabletest.TableApiTest;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.net.URL;

public class AggregateFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env, settings);

        URL resource = TableApiTest.class.getResource("/sensor.txt");
        String filePath = resource.getPath().toString();
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

        Table sensorTable = tabEnv.fromDataStream(dataStream, "id,temperature,timestamp.rowtime as ts");

        tabEnv.registerFunction("avgTmp",new AvgTemp());
        Table select = sensorTable.groupBy("id")
                .aggregate("avgTmp(temperature) as avgTemp")
                .select("id,avgTemp");
        tabEnv.toRetractStream(select, Row.class).print("tst");

        env.execute("agg func");
    }
}
