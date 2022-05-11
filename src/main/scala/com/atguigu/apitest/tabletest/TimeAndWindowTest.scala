package com.atguigu.apitest.tabletest

import com.atguigu.apitest.SensorReading
import com.xq.tabletest.TableApiTest
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

import java.net.URL
import java.time.Duration

/**
  * Copyright (c) 2018-2028 hr All Rights Reserved
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest.tabletest
  * Version: 1.0
  *
  * Created by hr on 2020/8/11 14:18
  */
object TimeAndWindowTest {
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
//    val inputPath = "D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    val resource: URL = classOf[TableApiTest].getResource("/sensor.txt")
    val filePath: String = resource.getPath.toString
    val inputStream = env.readTextFile(filePath)
    //    val inputStream = env.socketTextStream("localhost", 7777)

    val watermarkStrategy = WatermarkStrategy
      .forBoundedOutOfOrderness(Duration.ofSeconds(10))
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
      /*.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
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

    // 1. Group Window
    // 1.1 table api
    val resultTable = sensorTable
      .window(Tumble over 10.seconds on 'ts as 'tw)     // 每10秒统计一次，滚动时间窗口
      .groupBy('id, 'tw)
      .select('id, 'id.count, 'temperature.avg, 'tw.end)

    // 1.2 sql
    tableEnv.createTemporaryView("sensor", sensorTable)
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select
        |  id,
        |  count(id),
        |  avg(temperature),
        |  tumble_end(ts, interval '10' second)
        |from sensor
        |group by
        |  id,
        |  tumble(ts, interval '10' second)
      """.stripMargin)

    // 2. Over window：统计每个sensor每条数据，与之前两行数据的平均温度
    // 2.1 table api
    val overResultTable = sensorTable
      .window( Over partitionBy 'id orderBy 'ts preceding 2.rows as 'ow )
      .select('id, 'ts, 'id.count over 'ow, 'temperature.avg over 'ow)

    // 2.2 sql
    val overResultSqlTable = tableEnv.sqlQuery(
      """
        |select
        |  id,
        |  ts,
        |  count(id) over ow,
        |  avg(temperature) over ow
        |from sensor
        |window ow as (
        |  partition by id
        |  order by ts
        |  rows between 2 preceding and current row
        |)
      """.stripMargin)

    // 转换成流打印输出
    overResultTable.toAppendStream[Row].print("result")
//    overResultSqlTable.toRetractStream[Row].print("sql")

//    sensorTable.printSchema()
//    sensorTable.toAppendStream[Row].print()

    env.execute("time and window test")
  }
}
