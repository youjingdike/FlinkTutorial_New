package com.atguigu.apitest.sinttest

import com.atguigu.apitest.SensorReading
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaRecordSerializationSchemaBuilder, KafkaSink, KafkaSinkBuilder}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner

/**
  * Copyright (c) 2018-2028 hr All Rights Reserved
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest.sinttest
  * Version: 1.0
  *
  * Created by hr on 2020/8/7 10:12
  */
object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 读取数据
//    val inputPath = "D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
//    val inputStream = env.readTextFile(inputPath)

    // 从kafka读取数据
    /*val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")*/

    val kafkaSource:KafkaSource[String] = KafkaSource.builder()
      .setBootstrapServers("localhost:9092")
      .setTopics("sensor")
      .setGroupId("consumer-group")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setDeserializer(KafkaRecordDeserializationSchema.valueOnly[String](new SimpleStringSchema()))
      .build()
    val stream = env.fromSource(kafkaSource,WatermarkStrategy.noWatermarks(),"kakfa source")

//    val stream = env.addSource( new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties) )


    // 先转换成样例类类型（简单转换操作）
    val dataStream = stream
      .map( data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble).toString
      } )


    val serSchema : KafkaRecordSerializationSchemaBuilder[String] = KafkaRecordSerializationSchema.builder()
      .setTopic("sinktest")
      .setValueSerializationSchema(new SimpleStringSchema())
      .setPartitioner(new FlinkFixedPartitioner())

    val kafkaSinkBuilder : KafkaSinkBuilder[String] = KafkaSink.builder()
//      .setKafkaProducerConfig(properties)
      .setBootstrapServers("localhost:9092")
      .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
      .setRecordSerializer(serSchema.build())
    dataStream.sinkTo(kafkaSinkBuilder.build()).name("tstsink").uid("tstsink")
//    dataStream.addSink( new FlinkKafkaProducer011[String]("localhost:9092", "sinktest", new SimpleStringSchema()) )

    env.execute("kafka sink test")
  }
}
