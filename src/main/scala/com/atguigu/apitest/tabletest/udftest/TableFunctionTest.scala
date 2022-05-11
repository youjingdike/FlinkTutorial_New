package com.atguigu.apitest.tabletest.udftest

import com.atguigu.apitest.SensorReading
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

import java.time.Duration

/**
  * Copyright (c) 2018-2028 hr All Rights Reserved
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest.tabletest.udftest
  * Version: 1.0
  *
  * Created by hr on 2020/8/11 17:20
  */
object TableFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val settings = EnvironmentSettings.newInstance()
//      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    // 读取数据
    val inputPath = "D:\\code\\FlinkTutorial_1.10_New\\src\\main\\resources\\sensor.txt"
    val inputStream = env.readTextFile(inputPath)
    //    val inputStream = env.socketTextStream("localhost", 7777)

    val watermarkStrategy = WatermarkStrategy
      .forBoundedOutOfOrderness(Duration.ofSeconds(1))
      .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
        override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = element.timestamp * 1000L
      })

    // 先转换成样例类类型（简单转换操作）
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })
      .assignTimestampsAndWatermarks(watermarkStrategy)
     /* .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      })*/

    val schema = Schema.newBuilder()
      .column("id", "STRING")
      .column("temperature", "DOUBLE")
      //      .columnByExpression("temp1", "temperature")
      //      .columnByExpression("temp", "cast(temperature as DOUBLE)")
      //.column("timestamp", DataTypes.BIGINT())
      .columnByExpression("ts", Expressions.callSql("TO_TIMESTAMP_LTZ(`timestamp`, 0)"))
      //      .columnByExpression("ps", "PROCTIME()") //事件时间，类是字段与sql关键字冲突，要加上`号
      //操作时间
      //      .watermark("ts", "SOURCE_WATERMARK()")
      //.watermark("ts", sourceWatermark())
      .build()
//    val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime as 'ts)
    val sensorTable = tableEnv.fromDataStream(dataStream, schema)

    // 1. table api
    val split = new Split("_")     // new一个UDF实例
    val resutTable = sensorTable
      .joinLateral( split('id) as ('word, 'length))
      .select('id, 'ts, 'word, 'length)


    // 2. sql
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("split", split)
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select
        |  id, ts, word, length
        |from
        |  sensor, lateral table( split(id) ) as splitid(word, length)
      """.stripMargin)

    resutTable.toAppendStream[Row].print("result")
    resultSqlTable.toAppendStream[Row].print("sql")

    env.execute("table function test")

  }
}

// 自定义TableFunction
class Split(separator: String) extends TableFunction[(String, Int)]{
  def eval(str: String): Unit ={
    str.split(separator).foreach(
      word => collect((word, word.length))
    )
  }
}