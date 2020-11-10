package com.xq.tabletest.udftest;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableAggregateFunction;

/**
 * 自定义一个表聚合函数，实现Top2功能，输出（temp，rank）
 */
public class Top2Temp extends TableAggregateFunction<Tuple2<Double,Integer>,Top2TempAcc> {
    @Override
    public Top2TempAcc createAccumulator() {
        return null;
    }
}
