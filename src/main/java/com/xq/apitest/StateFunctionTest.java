package com.xq.apitest;

import com.xq.apitest.pojo.SensorReading;
import com.xq.apitest.sinktest.FileSinkTest;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StateFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStateBackend(new RocksDBStateBackend(""));
        env.setStateBackend(new FsStateBackend(""));
//        env.enableCheckpointing(60*1000L);//等价于：checkpointConfig.setCheckpointInterval(60*1000L);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setCheckpointInterval(60*1000L);
        checkpointConfig.setCheckpointTimeout(60000);
        checkpointConfig.setMaxConcurrentCheckpoints(2);
        checkpointConfig.setMinPauseBetweenCheckpoints(500L);
        checkpointConfig.setPreferCheckpointForRecovery(true);
        checkpointConfig.setTolerableCheckpointFailureNumber(0);

//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,10000L));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.minutes(5),Time.seconds(10)));


        URL resource = FileSinkTest.class.getResource("/sensor.txt");
        DataStreamSource<String> inputStream = env.readTextFile(resource.getPath().toString());
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0].trim(), Long.parseLong(split[1].trim()), Double.parseDouble(split[2].trim()));
            }
        });

        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");
        SingleOutputStreamOperator<String> mapStream = keyedStream.map(new MyRichMapper());
        mapStream.print("map stream");
        env.execute("test");
    }
}