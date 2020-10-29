package com.xq.tabletest;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.net.URL;

public class EsOutputTest {
    public static void main(String[] args) throws Exception {
        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 连接文件，注册表
        URL resource = FileOutputTest.class.getResource("/sensor.txt");
        String filePath = resource.getPath().toString();

        tableEnv.connect(new FileSystem().path(filePath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp",DataTypes.BIGINT())
                        .field("temp",DataTypes.DOUBLE())
//                        .field("pt",DataTypes.TIMESTAMP(3)).proctime()
                )
                .createTemporaryTable("inputTable");

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

        // 4. 输出到es
        tableEnv.connect(new Elasticsearch()
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
                .createTemporaryTable("esOutputTable");

        aggTable.insertInto("esOutputTable");

        tableEnv.toAppendStream(resultTable,new TupleTypeInfo<>(Types.STRING,Types.DOUBLE)).print("result");
        tableEnv.toRetractStream(aggTable, Row.class).print("agg");

        tableEnv.execute("es output test");
    }
}
