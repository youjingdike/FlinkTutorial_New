package com.xq.apitest.sinktest;

import com.xq.apitest.pojo.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Properties;

public class KafkaSourceAndSinkTest {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration conf = new Configuration();
        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER,true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node102:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
//        properties.setProperty("auto.offset.reset", "earliest");
// 从文件读取数据
//        DataStreamSource<String> inputStream = env.readTextFile("D:\\code\\FlinkTutorial_1.10_New\\src\\main\\resources\\sensor.txt");
//        DataStreamSource<String> inputStream = env.addSource(new FlinkKafkaConsumer011<String>("sensor", new SimpleStringSchema(), properties));
        DataStreamSource<SensorReading> inputStream = env.addSource(new FlinkKafkaConsumer011<SensorReading>("sensor", new DeserializationSchema<SensorReading>() {
            @Override
            public SensorReading deserialize(byte[] bytes) throws IOException {
                String value = new String(bytes, Charset.forName("UTF-8"));
                String[] split = value.split(",");
                return new SensorReading(split[0].trim(), Long.parseLong(split[1].trim()), Double.parseDouble(split[2].trim()));
            }

            @Override
            public boolean isEndOfStream(SensorReading sensorReading) {
                return false;
            }

            @Override
            public TypeInformation<SensorReading> getProducedType() {
                return TypeInformation.of(SensorReading.class);
            }
        }, properties));

        // 1. 基本转换操作：map成样例类类型
        SingleOutputStreamOperator<String> dataStream = inputStream.map((MapFunction<SensorReading, String>) value -> {
            return value.toString();
        });
        /*SingleOutputStreamOperator<String> dataStream = inputStream.map((MapFunction<String, String>) value -> {
            String[] split = value.split(",");
            return new SensorReading(split[0].trim(), Long.parseLong(split[1].trim()), Double.parseDouble(split[2].trim())).toString();
        });*/

        // 直接写入文件
        dataStream.writeAsText("D:\\code\\FlinkTutorial_1.10_New\\src\\main\\resources\\out");

//        dataStream.addSink( StreamingFileSink.forRowFormat(
////                new Path("D:\\code\\FlinkTutorial_1.10\\src\\main\\resources\\out1"),
//                new Path("hdfs://node101:9000/flink/hdfsSink"),
//                new SimpleStringEncoder<String>("UTF-8")
//        ).build());
        /*dataStream.addSink(new FlinkKafkaProducer011<String>("node102:9092","test",new SimpleStringSchema()))
        .name("kafka sink");*/
        env.execute("test kafka source and sink job");
    }
}
