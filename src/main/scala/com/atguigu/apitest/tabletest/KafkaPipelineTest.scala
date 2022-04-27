package com.atguigu.apitest.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
  * Copyright (c) 2018-2028 hr All Rights Reserved
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest.tabletest
  * Version: 1.0
  *
  * Created by hr on 2020/8/11 9:11
  */
object KafkaPipelineTest {
  def main(args: Array[String]): Unit = {
    // 1. 创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)

    // 2. 从kafka读取数据
    val sourceDescriptor: TableDescriptor = TableDescriptor.forConnector("kafka")
      .schema(Schema.newBuilder
        .column("id", DataTypes.STRING)
        .column("timestamp", DataTypes.BIGINT)
        .column("temperature", DataTypes.DOUBLE)
        .build)
      .option("topic", "sensor")
      .option("bootstrap.servers", "localhost:2181")
      .option("group.id", "testGroup")
      .format("csv")
      .build
    tableEnv.createTemporaryTable("kafkaInputTable", sourceDescriptor)
    /*tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("sensor")
      .property("zookeeper.connect", "localhost:2181")
      .property("bootstrap.servers", "localhost:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable")*/

    // 3. 查询转换
    // 3.1 简单转换
    val sensorTable = tableEnv.from("kafkaInputTable")
    val resultTable = sensorTable
      .select('id, 'temperature)
      .filter('id === "sensor_1")

    // 3.2 聚合转换
    val aggTable = sensorTable
      .groupBy('id)    // 基于id分组
      .select('id, 'id.count as 'count)

    // 4. 输出到kafka
    /*tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("sinktest")
      .property("zookeeper.connect", "localhost:2181")
      .property("bootstrap.servers", "localhost:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temp", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaOutputTable")*/
    val sinkDescriptor: TableDescriptor = TableDescriptor.forConnector("kafka")
      .schema(Schema.newBuilder
        .column("id", DataTypes.STRING)
        .column("temp", DataTypes.DOUBLE)
        .build)
      .option("topic", "sinkTest")
      .option("properties.bootstrap.servers", "localhost:2181")
      .option("properties.group.id", "testGroup")
      .format("csv")
      .build
    tableEnv.createTemporaryTable("kafkaOutputTable", sinkDescriptor)
    resultTable.executeInsert("kafkaOutputTable")

    env.execute("kafka pipeline test")
  }
}
