package com.xq.tabletest;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.FormatDescriptor;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.net.URL;

import static org.apache.flink.table.api.DataTypes.TIMESTAMP;

public class FileOutputTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 连接文件，注册表
        URL resource = FileOutputTest.class.getResource("/sensor.txt");
        String filePath = resource.getPath().toString();

        /*tableEnv.connect(new FileSystem().path(filePath))
            .withFormat(new Csv())
            .withSchema(new Schema()
                    .field("id", DataTypes.STRING())
                    .field("timestamp",DataTypes.BIGINT())
                    .field("temp",DataTypes.DOUBLE())
                    .field("pt",DataTypes.TIMESTAMP(3)).proctime()
            )
                .createTemporaryTable("inputTable");*/
        final TableDescriptor sourceDescriptor = TableDescriptor.forConnector("filesystem")
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.STRING())
                        .column("timestamp", DataTypes.BIGINT())
                        .column("temperature", DataTypes.DOUBLE())
                        .column("pt", TIMESTAMP(3))
                        .build())
                .option("path", filePath)
                .format(FormatDescriptor.forFormat("csv")
                        .option("field-delimiter", ",")
                        .build())
                .build();
        tableEnv.createTemporaryTable("inputTable",sourceDescriptor);
        // 3. 转换操作
        // 3.1 简单转换
        final Table sensorTable = tableEnv.from("inputTable");
        Table resultTable = sensorTable
                .select("id,temp")
                .filter("id === 'sensor_1'");

        //3.2 聚合操作
        Table aggTable = sensorTable
                .groupBy("id")// 基于id分组
                .select("id,id.count as cnt");

        // 4. 输出到文件
        // 注册输出表
        String outputPath = "D:\\code\\FlinkTutorial_1.10_New\\src\\main\\resources\\output.txt";
        /*tableEnv.connect(new FileSystem().path(outputPath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                                .field("id", DataTypes.STRING())
                                .field("temperature", DataTypes.DOUBLE())
//        .field("cnt", DataTypes.DOUBLE())
                )
                .createTemporaryTable("outputTable");*/
        final TableDescriptor sinkDescriptor = TableDescriptor.forConnector("filesystem")
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.STRING())
                        .column("temperature", DataTypes.DOUBLE())
                        .column("cnt", DataTypes.DOUBLE())
                        .build())
                .option("path", outputPath)
                .format("csv")
                .build();
        tableEnv.createTemporaryTable("outputTable",sinkDescriptor);

        resultTable.insertInto("outputTable");
//        aggTable.insertInto("outputTable");//会报错：AppendStreamTableSink requires that Table has only insert changes.

        tableEnv.toAppendStream(resultTable,new TupleTypeInfo<>(Types.STRING,Types.DOUBLE)).print("result");
        tableEnv.toRetractStream(aggTable, Row.class).print("agg");

        tableEnv.execute("table api test");
    }
}
