package com.atguigu.apitest.tabletest.udftest

import org.apache.flink.api.scala._
import com.atguigu.apitest.SensorReading
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

import java.time.Duration

/**
  * Copyright (c) 2018-2028   All Rights Reserved
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest.tabletest.udftest
  * Version: 1.0
  *
  * Created by hr on 2020/6/1 15:38
  */
object AggregateFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    val resource = getClass.getClassLoader.getResource("sensor.txt")
    val filePath = resource.getPath
    val inputStream: DataStream[String] = env.readTextFile(filePath)
    //    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    val watermarkStrategy = WatermarkStrategy.forBoundedOutOfOrderness[SensorReading](Duration.ofMillis(1000L))
      .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
        override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = element.timestamp
      })

    // map成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
      .assignTimestampsAndWatermarks(watermarkStrategy)

    val schema = Schema.newBuilder()
      .columnByExpression("ts", "TO_TIMESTAMP_LTZ(`timestamp`, 0)")
      .column("id", "STRING")
      .column("temperature", "DOUBLE")
//      .columnByExpression("temp1", "temperature")
//      .columnByExpression("temp", "cast(temperature as DOUBLE)")
       //.column("timestamp", DataTypes.BIGINT())
      //.columnByExpression("ts", Expressions.callSql("TO_TIMESTAMP_LTZ(`timestamp`, 0)"))
//      .columnByExpression("ps", "PROCTIME()") //事件时间，类是字段与sql关键字冲突，要加上`号
     //操作时间
      .watermark("ts", "SOURCE_WATERMARK()")
      //.watermark("ts", sourceWatermark())
      .build()
    val sensorTable = tableEnv.fromDataStream(dataStream, schema)
    // 将流转换成表，直接定义时间字段
//    val sensorTable: Table = tableEnv.fromDataStream(dataStream, schema)

    // 先创建一个聚合函数的实例
    val avgTemp = new AvgTemp()

    // Table API 调用
    val resultTable = sensorTable
      .groupBy('id)
      .aggregate( avgTemp('temperature) as 'avgTemp )
      .select('id, 'avgTemp)

    resultTable.toRetractStream[Row].print("result")

    env.execute("agg udf test job")
  }

  // 专门定义一个聚合函数的状态类，用于保存聚合状态（sum，count）
  class AvgTempAcc {
    var sum: Double = 0.0
    var count: Int = 0
  }
  // 自定义一个聚合函数
  class AvgTemp extends AggregateFunction[Double, AvgTempAcc]{

    override def getValue(accumulator: AvgTempAcc): Double = accumulator.sum/accumulator.count

    override def createAccumulator(): AvgTempAcc = new AvgTempAcc()

    def accumulate(acc: AvgTempAcc, temp: Double): Unit ={
      acc.sum += temp
      acc.count += 1
    }
  }
}
