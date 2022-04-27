package com.atguigu.apitest.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.DataTypes.TIMESTAMP
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, tableConversions}
import org.apache.flink.types.Row

/**
  * Copyright (c) 2018-2028 hr All Rights Reserved
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest.tabletest
  * Version: 1.0
  *
  * Created by hr on 2020/8/10 16:30
  */
object FileOutputTest {
  def main(args: Array[String]): Unit = {
    // 1. 创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)

    // 2. 连接外部系统，读取数据，注册表
    val filePath = "D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt"

    val srcDesc = TableDescriptor.forConnector("filesystem")
      .schema(Schema.newBuilder
        .column("id", DataTypes.STRING)
        .column("timestamp", DataTypes.BIGINT)
        .column("temperature", DataTypes.DOUBLE)
        .column("pt", TIMESTAMP(3))
        .build)
      .option("path", filePath)
      .format(FormatDescriptor.forFormat("csv")
        .option("field-delimiter", ",")
        .build)
      .build
    tableEnv.createTemporaryTable("inputTable",srcDesc)
    /*tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temp", DataTypes.DOUBLE())
//        .field("pt", DataTypes.TIMESTAMP(3)).proctime()
      )
      .createTemporaryTable("inputTable")*/

    // 3. 转换操作
    val sensorTable = tableEnv.from("inputTable")
    // 3.1 简单转换
    val resultTable = sensorTable
      .select($"id", $"temp")
      .filter($"id" === "sensor_1")

    // 3.2 聚合转换
    val aggTable = sensorTable
      .groupBy('id)    // 基于id分组
      .select('id, 'id.count as 'count)

    // 4. 输出到文件
    // 注册输出表
    val outputPath = "D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\output.txt"

    /*tableEnv.connect(new FileSystem().path(outputPath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temperature", DataTypes.DOUBLE())
//        .field("cnt", DataTypes.BIGINT())
      )
      .createTemporaryTable("outputTable")*/
    val sinkDesc = TableDescriptor.forConnector("filesystem")
      .schema(Schema.newBuilder
        .column("id", DataTypes.STRING)
        .column("temperature", DataTypes.DOUBLE)
        .column("cnt", DataTypes.BIGINT())
        .build)
      .option("path", outputPath)
      .format(FormatDescriptor.forFormat("csv")
        .option("field-delimiter", ",")
        .build)
      .build
    tableEnv.createTemporaryTable("outputTable",sinkDesc)
    resultTable.executeInsert("outputTable")

    resultTable.toAppendStream[(String, Double)].print("result")
    aggTable.toRetractStream[Row].print("agg")

    env.execute()
  }
}
