package com.atguigu.apitest.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api._

/**
  * Copyright (c) 2018-2028 hr All Rights Reserved
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest.tabletest
  * Version: 1.0
  *
  * Created by hr on 2020/8/11 9:27
  */
object EsOutputTest {
  def main(args: Array[String]): Unit = {
    // 1. 创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)

    // 2. 连接外部系统，读取数据，注册表
    val filePath = "D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt"

    /*tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temp", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")*/
    val srcDesc = TableDescriptor.forConnector("filesystem")
      .schema(Schema.newBuilder
        .column("id", DataTypes.STRING)
        .column("timestamp", DataTypes.BIGINT)
        .column("temp", DataTypes.DOUBLE)
        .build)
      .option("path", filePath)
      .format(FormatDescriptor.forFormat("csv")
        .option("field-delimiter", ",")
        .build)
      .build
    tableEnv.createTemporaryTable("inputTable",srcDesc)

    // 3. 转换操作
    val sensorTable = tableEnv.from("inputTable")
    // 3.1 简单转换
    val resultTable = sensorTable
      .select('id, 'temp)
      .filter('id === "sensor_1")

    // 3.2 聚合转换
    val aggTable = sensorTable
      .groupBy('id) // 基于id分组
      .select('id, 'id.count as 'count)

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
      .createTemporaryTable("esOutputTable")*/
    val sinkDescriptor: TableDescriptor = TableDescriptor.forConnector("elasticsearch-6")
      .schema(Schema.newBuilder
        .column("id", DataTypes.STRING)
        .column("count", DataTypes.BIGINT)
        .build)
      .option("hosts", "http://localhost:9200")
      .option("index", "sensor")
      .format("json")
      .build
    tableEnv.createTemporaryTable("esOutputTable", sinkDescriptor)
    aggTable.executeInsert("esOutputTable")

    env.execute("es output test")

  }
}
