package com.atguigu.apitest.tabletest

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Schema
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import java.time.Instant

object TstTable {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)
    // create a DataStream
    val dataStream = env.fromElements(
      User("Alice", 4, Instant.ofEpochMilli(1000)),
      User("Bob", 6, Instant.ofEpochMilli(1001)),
      User("Alice", 10, Instant.ofEpochMilli(1002)))


    // === EXAMPLE 1 ===

    // derive all physical columns automatically
    println("1111111111111")
    val table = tableEnv.fromDataStream(dataStream)
    table.printSchema()
    // prints:
    // (
    //  `name` STRING,
    //  `score` INT,
    //  `event_time` TIMESTAMP_LTZ(9)
    // )


    // === EXAMPLE 2 ===

    // derive all physical columns automatically
    // but add computed columns (in this case for creating a proctime attribute column)

    println("2222222222222222222")
    val table1 = tableEnv.fromDataStream(
        dataStream,
        Schema.newBuilder()
          .columnByExpression("proc_time", "PROCTIME()")
          .build())
    table1.printSchema()
    // prints:
    // (
    //  `name` STRING,
    //  `score` INT NOT NULL,
    //  `event_time` TIMESTAMP_LTZ(9),
    //  `proc_time` TIMESTAMP_LTZ(3) NOT NULL *PROCTIME* AS PROCTIME()
    //)


    // === EXAMPLE 3 ===

    // derive all physical columns automatically
    // but add computed columns (in this case for creating a rowtime attribute column)
    // and a custom watermark strategy

    println("33333333333")
    val table2 =
    tableEnv.fromDataStream(
        dataStream,
        Schema.newBuilder()
          .columnByExpression("rowtime", "CAST(event_time AS TIMESTAMP_LTZ(3))")
          .watermark("rowtime", "rowtime - INTERVAL '10' SECOND")
          .build())
    table2.printSchema()
    // prints:
    // (
    //  `name` STRING,
    //  `score` INT,
    //  `event_time` TIMESTAMP_LTZ(9),
    //  `rowtime` TIMESTAMP_LTZ(3) *ROWTIME* AS CAST(event_time AS TIMESTAMP_LTZ(3)),
    //  WATERMARK FOR `rowtime`: TIMESTAMP_LTZ(3) AS rowtime - INTERVAL '10' SECOND
    // )


    // === EXAMPLE 4 ===

    // derive all physical columns automatically
    // but access the stream record's timestamp for creating a rowtime attribute column
    // also rely on the watermarks generated in the DataStream API

    // we assume that a watermark strategy has been defined for `dataStream` before
    // (not part of this example)
    println("444444444444444444")
    val table3 =
    tableEnv.fromDataStream(
        dataStream,
        Schema.newBuilder()
          .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
          .watermark("rowtime", "SOURCE_WATERMARK()")
          .build())
    table3.printSchema()
    // prints:
    // (
    //  `name` STRING,
    //  `score` INT,
    //  `event_time` TIMESTAMP_LTZ(9),
    //  `rowtime` TIMESTAMP_LTZ(3) *ROWTIME* METADATA,
    //  WATERMARK FOR `rowtime`: TIMESTAMP_LTZ(3) AS SOURCE_WATERMARK()
    // )


    // === EXAMPLE 5 ===

    // define physical columns manually
    // in this example,
    //   - we can reduce the default precision of timestamps from 9 to 3
    //   - we also project the columns and put `event_time` to the beginning

    println("5555555555555555555")
    val table4 =
      tableEnv.fromDataStream(
        dataStream,
        Schema.newBuilder()
          .column("event_time", "TIMESTAMP_LTZ(9)").withComment("dd")
          .column("name", "STRING").withComment("dd")
          .column("score", "INT").withComment("dd")
          .columnByExpression("rowtime", "CAST(event_time AS TIMESTAMP_LTZ(3))").withComment("dd")
          .watermark("rowtime", "SOURCE_WATERMARK()").withComment("dd")
          .build())
    table4.printSchema()
    // prints:
    // (
    //  `event_time` TIMESTAMP_LTZ(3) *ROWTIME*,
    //  `name` VARCHAR(200),
    //  `score` INT
    // )
    // note: the watermark strategy is not shown due to the inserted column reordering projection
    env.execute("tst")
  }
}

case class User(name: String, score: java.lang.Integer, event_time: java.time.Instant)