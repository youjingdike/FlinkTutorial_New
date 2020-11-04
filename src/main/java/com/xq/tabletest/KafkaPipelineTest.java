package com.xq.tabletest;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class KafkaPipelineTest {
    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 2. 从kafka读取数据
        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("sensor")
                .property("zookeeper.connect", "localhost:2181")
                .property("bootstrap.servers", "localhost:9092"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp",DataTypes.BIGINT())
                        .field("temperature",DataTypes.DOUBLE()))
                .createTemporaryTable("kafkaInputTable");

        // 3. 转换操作
        // 3.1 简单转换
        final Table sensorTable = tableEnv.from("kafkaInputTable");
        Table resultTable = sensorTable
                .select("id,temperature")
                .filter("id === 'sensor_1'");

        //3.2 聚合操作
        Table aggTable = sensorTable
                .groupBy("id")// 基于id分组
                .select("id,id.count as cnt");

        // 2. 从kafka读取数据
        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("sinkTest")
                .property("zookeeper.connect", "localhost:2181")
                .property("bootstrap.servers", "localhost:9092"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temp",DataTypes.DOUBLE())
                )
                .createTemporaryTable("kafkaOutputTable");

        resultTable.insertInto("kafkaOutputTable");

        tableEnv.toAppendStream(resultTable,new TupleTypeInfo<>(Types.STRING,Types.DOUBLE)).print("result");
        tableEnv.toRetractStream(aggTable, Row.class).print("agg");

        tableEnv.execute("kafka pipeline test");
    }
}
