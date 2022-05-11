package com.atguigu.apitest.tabletest.udftest

import com.atguigu.apitest.SensorReading
import com.atguigu.apitest.tabletest.udftest.AggregateFunctionTest.getClass
import com.xq.tabletest.TableApiTest
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

import java.net.URL
import java.time.Duration

/**
  * Copyright (c) 2018-2028 hr All Rights Reserved
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest.tabletest.udftest
  * Version: 1.0
  *
  * Created by hr on 2020/8/11 17:04
  */
object ScalarFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    // 读取数据
    val resource: URL = getClass.getClassLoader.getResource("sensor.txt")
    val inputPath: String = resource.getPath
//    val inputPath = "D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    val inputStream = env.readTextFile(inputPath)
    //    val inputStream = env.socketTextStream("localhost", 7777)

    val watermarkStrategy = WatermarkStrategy.forBoundedOutOfOrderness[SensorReading](Duration.ofMillis(1000L))
      .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
        override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = element.timestamp
      })
    // 先转换成样例类类型（简单转换操作）
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })
      .assignTimestampsAndWatermarks(watermarkStrategy)

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

    // 调用自定义hash函数，对id进行hash运算
    // 1. table api
    // 首先new一个UDF的实例
    val hashCode = new HashCode(23)
    val resultTable = sensorTable
      .select('id, 'ts, hashCode('id))

    // 2. sql
    // 需要在环境中注册UDF
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.createTemporarySystemFunction("hashCode", hashCode)
    val resultSqlTable = tableEnv.sqlQuery("select id, ts, hashCode(id) from sensor")

    resultTable.toAppendStream[Row].print("result")
    resultSqlTable.toAppendStream[Row].print("sql")

    env.execute("scalar function test")
  }
}

// 自定义标量函数
class HashCode( factor: Int ) extends ScalarFunction{
  def eval( s: String ): Int = {
    s.hashCode * factor - 10000
  }
}
