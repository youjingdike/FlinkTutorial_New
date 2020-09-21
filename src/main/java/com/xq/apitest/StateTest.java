package com.xq.apitest;

import com.xq.apitest.pojo.SensorReading;
import com.xq.apitest.sinktest.FileSinkTest;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
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
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URL;

public class StateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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
        keyedStream.print();
        env.execute("test");
    }
}

// Keyed state测试：必须定义在RichFunction中，因为需要运行时上下文
//定义tuple的TypeInformation方式:TypeInformation.of(new TypeHint<Tuple3<String, Double, Integer>>() {})
class MyRichMapper extends RichMapFunction<SensorReading, String> {
    private ValueState<Double> valueState = null;
    private ListState<Integer> listState = null;
    private MapState<String,Double> mapState = null;
    private ReducingState<SensorReading> reducingState = null;
    private AggregatingState<SensorReading,String> aggregatingState = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        valueState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("valueState",Double.class));
        listState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("listState", Integer.class));
        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>("mapstate",String.class,Double.class));


        reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<SensorReading>("reducingState", new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                return new SensorReading(value1.getId(),value2.getTimestamp(),Math.min(value1.getTemperature(), value2.getTemperature()));
            }
        },SensorReading.class));


        aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<SensorReading, Tuple3<String,Double,Integer>, String>("aggstate",
                new AggregateFunction<SensorReading, Tuple3<String,Double,Integer>, String>() {

                    @Override
                    public Tuple3<String, Double, Integer> createAccumulator() {
                        return null;
                    }

                    @Override
                    public Tuple3<String, Double, Integer> add(SensorReading value, Tuple3<String, Double, Integer> accumulator) {
                        return null;
                    }

                    @Override
                    public String getResult(Tuple3<String, Double, Integer> accumulator) {
                        return null;
                    }

                    @Override
                    public Tuple3<String, Double, Integer> merge(Tuple3<String, Double, Integer> a, Tuple3<String, Double, Integer> b) {
                        return null;
                    }
                },
                TypeInformation.of(new TypeHint<Tuple3<String, Double, Integer>>() {})));
    }

    @Override
    public String map(SensorReading value) throws Exception {
        return null;
    }

}