package com.xq.tabletest.udftest;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

/**
 * 自定义一个表聚合函数，实现Top2功能，输出（temp，rank）
 */
public class Top2Temp extends TableAggregateFunction<Tuple2<Double,Integer>,Top2TempAcc> {
    @Override
    public Top2TempAcc createAccumulator() {
        return new Top2TempAcc();
    }

    public void accumulate(Top2TempAcc acc, Double temp) {
        if (acc.getHighestTemp()< temp) {
            acc.setSecondHighestTemp(acc.getHighestTemp());
            acc.setHighestTemp(temp);
        } else if (acc.getSecondHighestTemp() < temp) {
            acc.setSecondHighestTemp(temp);
        }
    }

    public void emitValue(Top2TempAcc acc, Collector<Tuple2<Double,Integer>> out) {
        out.collect(new Tuple2<>(acc.getHighestTemp(),1));
        out.collect(new Tuple2<>(acc.getSecondHighestTemp(),2));

    }
}
