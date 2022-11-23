package com.xq.apitest.sink;

import com.xq.apitest.pojo.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

public class StreamingFileSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(3000, CheckpointingMode.EXACTLY_ONCE);
//        env.setStateBackend(new FsStateBackend("file:///Users/xingqian/flink_tst/chkpoint"));
//        env.setStateBackend(new FsStateBackend("hdfs://localhost:9000/flink/chkpoint"));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://localhost:9000/flink/chkpoint");
        /*URL resource = StreamingFileSinkTest.class.getResource("/sensor.txt");
        DataStreamSource<String> inputStream = env.readTextFile(resource.getPath().toString());
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0].trim(), Long.parseLong(split[1].trim()), Double.parseDouble(split[2].trim()));
            }
        });*/

//        dataStream.print();

        StreamingFileSink<SensorReading> sink1 = StreamingFileSink.forRowFormat(
                new Path("D:\\code\\FlinkTutorial_1.10_New\\src\\main\\resources\\out_1"),
                new SimpleStringEncoder<SensorReading>("UTF-8")
        ).withBucketAssigner(new BasePathBucketAssigner<>())
         .withRollingPolicy(DefaultRollingPolicy.builder().build())
         .build();

        StreamingFileSink<SensorReading> sink2 = StreamingFileSink.forRowFormat(
                new Path("D:\\code\\FlinkTutorial_1.10_New\\src\\main\\resources\\out_2"),
                new SimpleStringEncoder<SensorReading>("UTF-8")
        ).withBucketAssigner(new DateTimeBucketAssigner<>())
         .withRollingPolicy(DefaultRollingPolicy.builder().build())
         .build();

        StreamingFileSink<SensorReading> sink3 = StreamingFileSink.forRowFormat(
                new Path("D:\\code\\FlinkTutorial_1.10_New\\src\\main\\resources\\out_3")
                ,new SimpleStringEncoder<SensorReading>()
        ).build();

        StreamingFileSink<SensorReading> sink4 = StreamingFileSink.forBulkFormat(
                new Path("D:\\code\\FlinkTutorial_1.10_New\\src\\main\\resources\\out_4")
                , ParquetAvroWriters.forReflectRecord(SensorReading.class)
        ).withBucketAssigner(new DateTimeBucketAssigner<>())
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

        StreamingFileSink<Row> sink5 = StreamingFileSink.forBulkFormat(
                        new Path("hdfs://localhost:9000/tst0")
                        , ParquetAvroWriters.forReflectRecord(Row.class)
                ).withBucketAssigner(new DateTimeBucketAssigner<>())
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();
        /**
         * 报错：The writeAsCsv() method can only be used on data streams of tuples.
         */
//        map.writeAsCsv("D:\\code\\FlinkTutorial_1.10_New\\src\\main\\resources\\out_4.txt");

        DataStreamSource<String> input = env.addSource(new RichSourceFunction<String>() {
            private boolean isCancel = false;

            public void run(SourceContext<String> sourceContext) throws Exception {
                Random random = new Random();

                Map<Integer, Double> map = new HashMap(10);
                for (int i = 0; i < 10; i++) {
                    map.put(i, 60 + random.nextGaussian() * 20);
                }

                while (!isCancel) {
                    Iterator<Map.Entry<Integer, Double>> iterator = map.entrySet().iterator();
                    for (; iterator.hasNext(); ) {
                        Map.Entry<Integer, Double> next = iterator.next();
                        map.put(next.getKey(), next.getValue() + random.nextGaussian());
                    }
                    final long currentTimeMillis = System.currentTimeMillis();
                    map.forEach((k, v) -> {
                        System.out.println(k+":"+v);
                        sourceContext.collect(k.toString()+","+currentTimeMillis+","+v);
                    });
                    // 间隔200ms
                    Thread.sleep(200);
                }
            }

            public void cancel() {
                isCancel = true;
            }
        });

        SingleOutputStreamOperator<Row> dataStream1 = input.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String value) throws Exception {
                String[] split = value.split(",");
                return Row.of(split[0].trim(), Long.parseLong(split[1].trim()), Double.parseDouble(split[2].trim()));
            }
        });
        dataStream1.print("row:");
        dataStream1.addSink(sink5);

        env.execute("file sink test");


    }
}
