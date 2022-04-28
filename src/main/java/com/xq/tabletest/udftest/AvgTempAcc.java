package com.xq.tabletest.udftest;

/**
 * 专门定义一个聚合函数的状态类，用于保存聚合状态（sum，count）
 */
public class AvgTempAcc {
    public double sum = 0.0D;
    public int count = 0;
}
