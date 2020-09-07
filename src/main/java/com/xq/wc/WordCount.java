package com.xq.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.net.URL;

public class WordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        URL resource = WordCount.class.getResource("/sensor.txt");
        DataSource<String> dataSource = env.readTextFile(resource.getPath());
//        DataSource<String> dataSource = env.readTextFile("D:\\code\\FlinkTutorial_1.10\\src\\main\\resources\\sensor.txt");

        AggregateOperator<Tuple2<String, Integer>> resultData = dataSource.flatMap(new FlatMapFunction<String, String>() {
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] split = s.split(",");
                for (String ss : split) {
                    collector.collect(ss);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).groupBy(0)
                .sum(1);

        resultData.print();

    }

}
