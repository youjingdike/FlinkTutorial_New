package com.xq.apitest.api;

import com.xq.apitest.pojo.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class WindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100L);

        // 从文件读取数据
//        DataStreamSource<String> inputStream = env.readTextFile("D:\\code\\FlinkTutorial_1.10\\src\\main\\resources\\sensor.txt");
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 9999);

        // 1. 基本转换操作：map成样例类类型
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map((MapFunction<String, SensorReading>) value -> {
            String[] split = value.split(",");
            return new SensorReading(split[0].trim(), Long.parseLong(split[1].trim()), Double.parseDouble(split[2].trim()));
        })
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.milliseconds(1000L)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTimestamp()*1000L;
            }
        });
/*        SingleOutputStreamOperator<SensorReading> resultStream = dataStream.keyBy("id")
//                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
//                .timeWindow(Time.seconds(15))
//                .allowedLateness(Time.seconds(1))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(new OutputTag<SensorReading>("late"){})
                .reduce(new MyReduceFuc());*/
        SingleOutputStreamOperator<String> resultStream = dataStream.keyBy("id")
//                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
//                .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
//                .timeWindow(Time.seconds(15))
//                .allowedLateness(Time.seconds(1))
                .window(EventTimeSessionWindows.withGap(Time.seconds(1)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(new OutputTag<SensorReading>("late"){})
//                .reduce(new MyReduceFuc());
                .aggregate(new MyAggFun());

        dataStream.print("data");
        resultStream.print("result");
        resultStream.getSideOutput(new OutputTag<SensorReading>("late"){}).print("late");
        env.execute("window test job");
    }
}
//result> 3.0
//        result> 4.0
//        result> 36.275
//        result> 6.7
//        result> 15.4
//        result> 38.1
//        result> 34.505
//求最小值
class MyReduceFuc implements ReduceFunction<SensorReading> {

    @Override
    public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
        return new SensorReading(value1.getId(),value2.getTimestamp(),Math.min(value1.getTemperature(), value2.getTemperature()));
    }
}

class MyAggFun implements AggregateFunction<SensorReading, Tuple3<String,Double, Integer>, String> {

    @Override
    public Tuple3<String,Double, Integer> createAccumulator() {
        return new Tuple3<>("",0.0D,0);
    }

    @Override
    public Tuple3<String,Double, Integer> add(SensorReading value, Tuple3<String,Double, Integer> accumulator) {
        accumulator.f0 = value.getId();
        accumulator.f1 += value.getTemperature();
        accumulator.f2 += 1;
        return accumulator;
    }

    @Override
    public String getResult(Tuple3<String,Double, Integer> accumulator) {
        return accumulator.f0+":"+(accumulator.f1/accumulator.f2);
    }

    @Override
    public Tuple3<String,Double, Integer> merge(Tuple3<String,Double, Integer> a, Tuple3<String,Double, Integer> b) {
        return new Tuple3<>(a.f0,a.f1+b.f1,a.f2+b.f2);
    }
}