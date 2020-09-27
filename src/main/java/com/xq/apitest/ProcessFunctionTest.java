package com.xq.apitest;

import com.xq.apitest.pojo.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
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
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 9999);

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
//                .process(new MyProFunc());
                .process(new TempIncreWaining(5*1000L));

        process.print("process");
        env.execute("test process func");
    }
}

// 实现自定义的KeyedProcessFunction
class TempIncreWaining extends KeyedProcessFunction<String, SensorReading, String> {

    private Long interval;

    private ValueState<Double> lastTempState = null;
    //定义状态：保存上一个温度值进行比较，保存注册定时器的时间戳用于删除
    private ValueState<Long> timerTsState = null;

    public TempIncreWaining(Long interval) {
        this.interval = interval;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTempState",Double.class));
        timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTsState",Long.class));
    }

    @Override
    public void processElement(SensorReading sen, Context ctx, Collector<String> out) throws Exception {
        Double lastTemp = lastTempState.value();
        Long timerTs = timerTsState.value();
        lastTempState.update(sen.getTemperature());

        if (lastTemp!=null && timerTs==null && sen.getTemperature()>lastTemp) {
            long ts = ctx.timerService().currentProcessingTime() + interval;
            timerTsState.update(ts);
            ctx.timerService().registerProcessingTimeTimer(ts);
        } else if (timerTs != null && lastTemp != null && sen.getTemperature() < lastTemp) {
            ctx.timerService().deleteProcessingTimeTimer(timerTs);
            timerTsState.clear();
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        out.collect("传感器" + ctx.getCurrentKey() + "的温度连续" + interval/1000 + "秒连续上升");
        timerTsState.clear();
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
        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime()+5000L);
//        ctx.timerService().registerProcessingTimeTimer(ctx.timestamp()+6000L);
//        ctx.timerService().registerEventTimeTimer(ctx.timestamp()+6000L);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        System.out.println("执行timer");
    }
}