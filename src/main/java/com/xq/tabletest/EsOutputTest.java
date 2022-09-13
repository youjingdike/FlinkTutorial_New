package com.xq.tabletest;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.net.URL;

import static org.apache.flink.table.api.Expressions.$;

public class EsOutputTest {
    public static void main(String[] args) throws Exception {
        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 连接文件，注册表
        URL resource = EsOutputTest.class.getResource("/sensor.txt");
        String filePath = resource.getPath();

        final TableDescriptor sourceDescriptor = TableDescriptor.forConnector("filesystem")
                .schema(org.apache.flink.table.api.Schema.newBuilder()
                        .column("id", DataTypes.STRING())
                        .column("timestamp", DataTypes.BIGINT())
                        .column("temperature", DataTypes.DOUBLE())
                        .build())
                .option("path", filePath)
                .format("csv")
                .build();
        tableEnv.createTemporaryTable("inputTable",sourceDescriptor);

        // 3. 转换操作
        // 3.1 简单转换
        final Table sensorTable = tableEnv.from("inputTable");
        Table resultTable = sensorTable
                .select($("id"),$("temp"))
                .filter($("id").isEqual("sensor_1"));

        //3.2 聚合操作
        Table aggTable = sensorTable
                .groupBy($("id"))// 基于id分组
                .select($("id"),$("id").count().as("cnt"));

        // 4. 输出到es
        /*tableEnv.connect(new Elasticsearch()
                .version("6")
                .host("localhost", 9200, "http")
                .index("sensor")
                .documentType("temperature")
        )
                .inUpsertMode()
                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("count", DataTypes.BIGINT())
                )
                .createTemporaryTable("esOutputTable");*/
        final TableDescriptor sinkDescriptor = TableDescriptor.forConnector("elasticsearch-7")
                .schema(org.apache.flink.table.api.Schema.newBuilder()
                        .column("id", DataTypes.STRING())
                        .column("count", DataTypes.BIGINT())
                        .build())
                .option("hosts", "http://localhost:9200")
                .option("index","sensor")
                .format("json")
                .build();
        tableEnv.createTemporaryTable("esOutputTable",sinkDescriptor);
        aggTable.executeInsert("esOutputTable");

        tableEnv.toDataStream(resultTable).print("result");
        tableEnv.toChangelogStream(aggTable).print("agg");

        env.execute("es output test");
    }
}
