package com.xq.apitest.api;

import com.xq.apitest.pojo.SensorReading;
import com.xq.sources.RandomSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.CheckpointListener;
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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CheckpointTest {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(4);
//            env.setStateBackend(new FsStateBackend("", true))
//        env.setStateBackend(new RocksDBStateBackend("file:////D://checkpoint"));
//        env.setStateBackend(new MemoryStateBackend());

        env.setStateBackend(new HashMapStateBackend());
//        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.enableCheckpointing(10*1000L);//等价于：checkpointConfig.setCheckpointInterval(60*1000L);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
//        checkpointConfig.setCheckpointStorage(new JobManagerCheckpointStorage());
        checkpointConfig.setCheckpointStorage(new FileSystemCheckpointStorage("file:///Users/xingqian/checkpoint-dir"));
//        checkpointConfig.setCheckpointStorage("file:///checkpoint-dir");


        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setCheckpointInterval(1*1000L);
        checkpointConfig.setCheckpointTimeout(20*1000L);
        //设置同时可能正在进行的检查点尝试的最大次数。如果该值为n，则在当前有n个检查点尝试时不会触发检查点。
        // 对于要触发的下一个检查点，必须前面的一次检查点尝试需要完成或过期。
        checkpointConfig.setMaxConcurrentCheckpoints(4);
        checkpointConfig.setMinPauseBetweenCheckpoints(500L);
//        checkpointConfig.setPreferCheckpointForRecovery(true);
        //这定义了在整个作业发生故障转移之前，可以容忍多少个连续检查点故障。
        // 默认值为“0”，这意味着不容忍检查点失败，并且作业将在第一次报告检查点失败时失败。
        //这里虽然设置的是5，但是setMaxConcurrentCheckpoints设置的为4，当达到最大值5的时候，也不会立马重启任务，会等待已触发的cp,全部失败才认为任务失败重启
        checkpointConfig.setTolerableCheckpointFailureNumber(5);
//        checkpointConfig.setTolerableCheckpointFailureNumber(0);

//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,10000L));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.minutes(5),Time.seconds(10)));

        DataStreamSource<SensorReading> inputStream = env.addSource(new RandomSource());
        KeyedStream<SensorReading, String> keyedStream = inputStream
//                .keyBy("id");
                .keyBy(new KeySelector<SensorReading, String>() {
                    @Override
                    public String getKey(SensorReading sensorReading) throws Exception {
                        return sensorReading.getId();
                    }
                });

        // 需求：对于温度传感器温度值跳变，超过10度，报警
        // 用自定义RichFunction实现状态编程
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> mapStream = keyedStream
                .flatMap(new TempChangeAlert1(10D))
                .uid("tempChangeAlert");
        mapStream.print("map stream");



        env.execute("test");
    }
}

class TempChangeAlert1 extends RichFlatMapFunction<SensorReading,Tuple3<String,Double,Double>> implements CheckpointedFunction,CheckpointListener {

    private Double threshold;
    //保存上次的温度
    private ValueState<Double> lastTempState = null;
    //保存报警状态
    private ValueState<Boolean> isFirstTempState = null;

    public TempChangeAlert1(Double threshold) {
        this.threshold = threshold;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTempState",Double.class));
        isFirstTempState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isFirstTempState",Boolean.class));
    }

    @Override
    public void flatMap(SensorReading sen, Collector<Tuple3<String, Double, Double>> out) throws Exception {
        Double lastTemp = lastTempState.value();
        if (lastTemp==null) {
            lastTemp = 0D;
        }
        //与最新的温度求差作比较
        double diff = Math.abs(sen.getTemperature() - lastTemp);

        if (isFirstTempState.value() == null) {
            isFirstTempState.update(true);
        }

        if (!isFirstTempState.value() && diff > threshold) {
            out.collect(new Tuple3<>(sen.getId(), lastTemp, sen.getTemperature()));
        }
        isFirstTempState.update(false);
        lastTempState.update(sen.getTemperature());
    }


    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        System.out.println("...start complete:"+checkpointId);
        while (true){
            Thread.sleep(10000000000L);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.out.println("...start snapshot:"+context.getCheckpointId());
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
    }
}