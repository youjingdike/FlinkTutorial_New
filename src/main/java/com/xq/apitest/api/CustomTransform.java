package com.xq.apitest.api;

import com.xq.apitest.pojo.SensorReading;
import com.xq.sources.RandomSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class CustomTransform {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);

        SingleOutputStreamOperator<String> s1 = env.addSource(new RandomSource(false))
                .transform("custom",
                        TypeInformation.of(String.class),
                        new CustomTrans<SensorReading>());

        s1.print("s1");

        env.execute();
    }
}

class CustomTrans<IN> extends AbstractStreamOperator<java.lang.String>
        implements OneInputStreamOperator<IN, java.lang.String>, BoundedOneInput {

    @Override
    public void endInput() throws Exception {

    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        IN value = element.getValue();
        System.out.println("cusTrans:"+value.toString());
        output.collect(new StreamRecord<>(value.toString()));
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        while (true) {

        }
    }
}