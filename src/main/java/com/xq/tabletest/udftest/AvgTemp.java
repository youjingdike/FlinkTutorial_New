package com.xq.tabletest.udftest;

import org.apache.flink.table.functions.AggregateFunction;

public class AvgTemp extends AggregateFunction<Double,AvgTempAcc> {
    @Override
    public Double getValue(AvgTempAcc acc) {
        if (acc.count == 0) {
            return null;
        } else {
            return acc.sum / acc.count;
        }
    }

    @Override
    public AvgTempAcc createAccumulator() {
        return new AvgTempAcc();
    }

    public void accumulate(AvgTempAcc acc,Double temp) {
        acc.count=acc.count+1;
        acc.sum=acc.sum+temp;
    }
}
