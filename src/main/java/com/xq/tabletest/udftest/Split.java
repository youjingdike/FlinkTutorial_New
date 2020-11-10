package com.xq.tabletest.udftest;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableFunction;

public class Split extends TableFunction<Tuple2<String,Integer>> {
    private String separator = " ";

    public Split(String separator) {
        this.separator = separator;
    }

    public void eval(String s) {
        String[] split = s.split(separator);
        for (String s1 : split) {
            collect(new Tuple2<String,Integer>(s1,s1.length()));
        }
    }
}
