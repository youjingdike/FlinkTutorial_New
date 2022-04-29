package com.xq.tabletest;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.net.URL;
import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

public class TableApiTest {
    public static void main(String[] args) throws Exception {
        /*EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);*/

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        /*// 1.1 基于老版本planner的流处理
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment oldStreamTableEnv = StreamTableEnvironment.create(env, settings);

        //1.2 基于老版本的批处理
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment oldTable = BatchTableEnvironment.create(batchEnv);

        //1.3基于blink planner的流处理
        EnvironmentSettings blinkSetting = EnvironmentSettings.newInstance()
                .useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment blinkStreamTabEnv = StreamTableEnvironment.create(env, blinkSetting);

        //1.4基于blink planner的批处理
        EnvironmentSettings blinkBatchSetting = EnvironmentSettings.newInstance()
                .useBlinkPlanner().inBatchMode().build();
        StreamTableEnvironment blinkBatchTabEnv = StreamTableEnvironment.create(env, blinkBatchSetting);*/

        // 2. 连接外部系统，读取数据，注册表
        // 2.1 读取文件
        URL resource = TableApiTest.class.getResource("/sensor1.txt");
        String filePath = resource.getPath();
        final TableDescriptor sourceDescriptor = TableDescriptor.forConnector("filesystem")
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.STRING())
                        .column("timestamp", DataTypes.BIGINT())
                        .column("temperature", DataTypes.DOUBLE())
                        .column("rowtime", DataTypes.TIMESTAMP(3))
                        .columnByExpression("proctime","PROCTIME()")
                        .watermark("rowtime","rowtime - INTERVAL '5' SECOND")
                        .build())
                .option("path", filePath)
                .format("csv")
                .build();
        tableEnv.createTemporaryTable("inputTable",sourceDescriptor);
        /*tableEnv.connect(new FileSystem().path(filePath))
            .withFormat(new Csv())
            .withSchema(new Schema()
                    .field("id", DataTypes.STRING())
                    .field("timestamp",DataTypes.BIGINT())
                    .field("temperature",DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable");*/
        // 2.2 读取kafka
        /*tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("sensor")
                .property("zookeeper.connect", "localhost:2181")
                .property("bootstrap.servers", "localhost:9092"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp",DataTypes.BIGINT())
                        .field("temperature",DataTypes.DOUBLE()))
                .createTemporaryTable("kafkaInputTable");*/

        // 3. 查询转换
        // 3.1 使用table api
        final Table sensorTable = tableEnv.from("inputTable");
        Table resultTable = sensorTable
                .select($("id"),$("temperature"),$("rowtime"),$("proctime"))
                .filter($("id").isEqual("sensor_1"));

        // 3.2 SQL
        Table resultSqlTable = tableEnv.sqlQuery(
                "select id, temperature as temp from inputTable where id = 'sensor_1'");

//    val inputTable: Table = tableEnv.from("kafkaInputTable")
//    inputTable.toAppendStream[(String, Long, Double)].print()

        TableSchema schema = resultSqlTable.getSchema();
        int fieldCount = schema.getFieldCount();
        Arrays.stream(schema.getFieldNames()).forEach(System.out::println);
        System.out.println("~~~~~~~~~~~~~~~~~~~~");
        for (int i = 0; i < fieldCount; i++) {
            System.out.println(schema.getFieldName(i));
        }

//        tableEnv.toDataStream(resultTable,new TupleTypeInfo<>(Types.STRING,Types.DOUBLE))
//        tableEnv.toAppendStream(resultTable,new TupleTypeInfo<>(Types.STRING,Types.DOUBLE,Types.SQL_TIMESTAMP,Types.SQL_TIMESTAMP)).print("result");
        tableEnv.toDataStream(resultTable).print("result");
//        DataStream<Tuple> tupleDataStream = tableEnv.toAppendStream(resultSqlTable, new TupleTypeInfo<>(Types.STRING, Types.DOUBLE));
        DataStream tupleDataStream = tableEnv.toDataStream(resultSqlTable);
        tupleDataStream.print("sql");

        DataStream<Row> rowDataStream = tableEnv.toDataStream(resultSqlTable, Row.class);
        rowDataStream.addSink(new SinkFunction<Row>() {
            @Override
            public void invoke(Row value, Context context) throws Exception {
                System.out.println(value.getField(0));
            }
        });

        env.execute("table api test");
    }
}
