package com.xq.apitest.api;

import com.xq.apitest.pojo.SensorReading;
import com.xq.apitest.sinktest.FileSinkTest;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
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
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //    env.setStateBackend(new FsStateBackend("", true))
//        env.setStateBackend(new RocksDBStateBackend("file:////D://checkpoint"));
        env.setStateBackend(new MemoryStateBackend());
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

        KeyedStream<SensorReading, Tuple> keyedStream = dataStream
                .keyBy("id");

//        SingleOutputStreamOperator<String> mapStream = keyedStream
//                .map(new MyRichMapper())
//                .uid("myRichMapper");

        // 需求：对于温度传感器温度值跳变，超过10度，报警
        // 用自定义RichFunction实现状态编程
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> mapStream = keyedStream
                .flatMap(new TempChangeAlert(10D))
                .uid("tempChangeAlert");
        mapStream.print("map stream");



        env.execute("test");
    }
}

class TempChangeAlert extends RichFlatMapFunction<SensorReading,Tuple3<String,Double,Double>> {

    private Double threshold;
    //保存上次的温度
    private ValueState<Double> lastTempState = null;
    //保存报警状态
    private ValueState<Boolean> isFirstTempState = null;

    public TempChangeAlert(Double threshold) {
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
}

// Keyed state测试：必须定义在RichFunction中，因为需要运行时上下文
//定义tuple的TypeInformation方式:TypeInformation.of(new TypeHint<Tuple3<String, Double, Integer>>() {})
class MyRichMapper extends RichMapFunction<SensorReading, String> {
    private ValueState<Integer> valueState = null;
    private ListState<Double> listState = null;
    private MapState<String,Double> mapState = null;
    private ReducingState<SensorReading> reducingState = null;
    private AggregatingState<SensorReading,String> aggregatingState = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("valueState",Integer.class));
        listState = getRuntimeContext().getListState(new ListStateDescriptor<Double>("listState", Double.class));
        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>("mapState",String.class,Double.class));


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
                        return new Tuple3<>("",0.0D,0);
                    }

                    @Override
                    public Tuple3<String, Double, Integer> add(SensorReading value, Tuple3<String, Double, Integer> accumulator) {
                        accumulator.f0 = value.getId();
                        accumulator.f1 += value.getTemperature();
                        accumulator.f2 += 1;
                        return accumulator;
                    }

                    @Override
                    public String getResult(Tuple3<String, Double, Integer> accumulator) {
                        return accumulator.f0+":"+(accumulator.f1/accumulator.f2);
                    }

                    @Override
                    public Tuple3<String, Double, Integer> merge(Tuple3<String, Double, Integer> a, Tuple3<String, Double, Integer> b) {
                        return new Tuple3<>(a.f0,a.f1+b.f1,a.f2+b.f2);
                    }
                },
                TypeInformation.of(new TypeHint<Tuple3<String, Double, Integer>>() {})));
    }

    @Override
    public String map(SensorReading sensorReading1) throws Exception {
        Integer value1 = valueState.value();
        if (value1 != null) {
            System.out.println("valueState:"+sensorReading1.getId()+ "->" + value1.toString());
            valueState.update(++value1);
        } else {
            valueState.update(1);
            System.out.println("valueState:"+sensorReading1.getId()+ "-> 0");
        }

        Iterable<Double> integerIterable = listState.get();
        integerIterable.forEach(integer -> System.out.println("listState:"+integer));
        listState.add(sensorReading1.getTemperature());

        if (value1!=null&&value1>2) {
            System.out.println("###");
            List<Double> list = new ArrayList<>();
            list.add(1.0D);
            list.add(1.1D);
            list.add(1.2D);
            listState.update(list);
        }

        Iterable<Map.Entry<String, Double>> entries = mapState.entries();
        mapState.put(""+sensorReading1.getId(),sensorReading1.getTemperature());

        reducingState.add(sensorReading1);
        SensorReading sensorReading = reducingState.get();
        System.out.println(sensorReading1.getId()+":mixTemp:"+sensorReading.getTemperature());

        aggregatingState.add(sensorReading1);
        aggregatingState.get();
        return aggregatingState.get();
    }

}