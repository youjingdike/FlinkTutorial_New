package com.xq.tabletest.udftest;

/**
 * 自定义状态类
 */
public class Top2TempAcc {
        private Double highestTemp = Double.MIN_VALUE;
        private Double secondHighestTemp = Double.MIN_VALUE;

        public Double getHighestTemp() {
        return highestTemp;
    }

        public void setHighestTemp(Double highestTemp) {
        this.highestTemp = highestTemp;
    }

        public Double getSecondHighestTemp() {
        return secondHighestTemp;
    }

        public void setSecondHighestTemp(Double secondHighestTemp) {
        this.secondHighestTemp = secondHighestTemp;
    }
}
