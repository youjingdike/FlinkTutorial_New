package com.xq.apitest.api;

import com.xq.apitest.pojo.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        URL resource = ProcessFunctionTest.class.getResource("/sensor.txt");
//        DataStreamSource<String> inputStream = env.readTextFile(resource.getPath().toString());
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0].trim(), Long.parseLong(split[1].trim()), Double.parseDouble(split[2].trim()));
            }
        });

        SingleOutputStreamOperator<SensorReading> process = dataStream
                .process(new SplitTemp(30.0D));

        process.print("process");

        process.getSideOutput(new OutputTag<Tuple3<String, Long, Double>>("low") {
        }).print("sideOutput");
        env.execute("test sideOutput func");
    }
}

// 实现自定义ProcessFunction，进行分流
class SplitTemp extends ProcessFunction<SensorReading, SensorReading> {

    private Double threshold;

    public SplitTemp(Double threshold) {
        this.threshold = threshold;
    }


    @Override
    public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
        if( value.getTemperature() > threshold ){
            // 如果当前温度值大于30，那么输出到主流
            out.collect(value);
        } else {
            // 如果不超过30度，那么输出到侧输出流
            ctx.output(new OutputTag<Tuple3<String, Long, Double>>("low") {}, new Tuple3<>(value.getId(), value.getTimestamp(), value.getTemperature()));
        }
    }
}