package com.xq.tabletest;

import com.xq.apitest.pojo.SensorReading;
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

public class TimeAndWindowTest {
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

        // 1. Group Window
        // 1.1 table api
        /*Table resultTable = sensorTable
                .window(Tumble.over("10.seconds").on("ts").as("tw"))// 每10秒统计一次，滚动时间窗口
                .groupBy("id,tw")
                .select("id, id.count, temperature.avg, tw.end");

        DataStream<Tuple> tupleDataStream = tabEnv.toAppendStream(resultTable, new TupleTypeInfo<>(Types.STRING, Types.LONG, Types.DOUBLE, Types.SQL_TIMESTAMP));
        tupleDataStream.print("Append");
        tupleDataStream.addSink(new SinkFunction<Tuple>() {
            @Override
            public void invoke(Tuple value, Context context) throws Exception {
                System.out.println("size:"+value.getArity());
                System.out.println("value:"+value.getField(0)+":"+value.getField(1));
            }
        });

        DataStream<Row> rowDataStream = tabEnv.toAppendStream(resultTable, Row.class);
        rowDataStream.print("Retract");
        rowDataStream.addSink(new SinkFunction<Row>() {
            @Override
            public void invoke(Row value, Context context) throws Exception {
                System.out.println("rowsize:"+value.getArity());
                System.out.println("value:"+value.getField(0)+":"+value.getField(1));
            }
        });*/


        // 1.2 sql
        /*tabEnv.createTemporaryView("sensorTable",sensorTable);
        Table resultSqlTable = tabEnv.resultSqlTable("select id,count(id),avg(temperature),tumble_end(ts, interval '10' second)" +
                " from sensorTable group by id,  tumble(ts, interval '10' second) ");
//        resultSqlTable.printSchema();
        tabEnv.toAppendStream(resultSqlTable, Row.class).print("sql");*/

        //2. Over window：统计每个sensor每条数据，与之前两行数据的平均温度
        //2.1 table api
        //Over partitionBy 'id orderBy 'ts preceding 2.rows as 'ow
        /*Table overTalbe = sensorTable
                .window(Over.partitionBy("id").orderBy("ts").preceding("2.rows").as("ow"))
                .select("id,ts,id.count over ow,temperature.avg over ow");
        tabEnv.toAppendStream(overTalbe, Row.class).print("ov");*/
        //2.2 sql
        tabEnv.createTemporaryView("sensorTable",sensorTable);
        Table sqlOverTable = tabEnv.sqlQuery("select id,ts,count(id) over ow,avg(temperature) over ow from sensorTable " +
                "window ow as (" +
                "  partition by id" +
                "  order by ts" +
                "  rows between 2 preceding and current row)");
        tabEnv.toAppendStream(sqlOverTable, Row.class).print("sql ov");
        tabEnv.execute("time and window test");
    }
}
