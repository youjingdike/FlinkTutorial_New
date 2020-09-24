package com.xq.apitest;

import com.xq.apitest.pojo.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.net.URL;

public class ProcessFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        URL resource = ProcessFunctionTest.class.getResource("/sensor.txt");
//        DataStreamSource<String> inputStream = env.readTextFile(resource.getPath().toString());
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0].trim(), Long.parseLong(split[1].trim()), Double.parseDouble(split[2].trim()));
            }
        });

        SingleOutputStreamOperator<String> process = dataStream
//                .keyBy("id")  //如果用属性名，对应的key的泛型是Tuple
                .keyBy(new KeySelector<SensorReading, String>() {
                    @Override
                    public String getKey(SensorReading value) throws Exception {
                        return value.getId();
                    }
                })
                .process(new MyProFunc());

        process.print("process");
        env.execute("test process func");
    }
}


// KeyedProcessFunction功能测试
class MyProFunc extends KeyedProcessFunction<String, SensorReading, String> {
    private ValueState<Integer> myState;

    @Override
    public void open(Configuration parameters) throws Exception {
        myState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("my-state", Integer.class));
    }

    @Override
    public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
        System.out.println(ctx.getCurrentKey());
        System.out.println(ctx.timestamp());
        System.out.println(ctx.timerService().currentWatermark());
        System.out.println(ctx.timerService().currentProcessingTime());
        ctx.timerService().registerProcessingTimeTimer(3000L);
//        ctx.timerService().registerProcessingTimeTimer(ctx.timestamp()+6000L);
//        ctx.timerService().registerEventTimeTimer(ctx.timestamp()+6000L);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        System.out.println("执行timer");
    }
}