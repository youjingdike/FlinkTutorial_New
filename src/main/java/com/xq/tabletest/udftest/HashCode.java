package com.xq.tabletest.udftest;

import org.apache.flink.table.functions.ScalarFunction;

public class HashCode  extends ScalarFunction {
    private int factor;
    public HashCode(int factor) {
        this.factor = factor;
    }

    public int eval(String s) {
        return s.hashCode() * factor - 10000;
    }
}
