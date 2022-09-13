package com.xq.apitest.api;

import com.xq.sources.RandomSource;
import com.xq.apitest.pojo.SensorReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class SourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        Configuration conf = new Configuration();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);

//        env.registerCachedFile("fff","dd");

//        DataStreamSource<SensorReading> s1 = env.fromCollection(Arrays.asList(new SensorReading("sensor_1", 1547718199L, 35.8D),
//                new SensorReading("sensor_6", 1547718201L, 15.4D),
//                new SensorReading("sensor_7", 1547718202L, 6.7D),
//                new SensorReading("sensor_10", 1547718205L, 38.1D)));
//
//        DataStreamSource<String> s2 = env.fromElements("3a","5dd");

        DataStreamSource<SensorReading> s3 = env.addSource(new RandomSource());

//        s1.print("s1");
//        s2.print("s2");
        s3.print("s3");

        env.execute();
    }
}