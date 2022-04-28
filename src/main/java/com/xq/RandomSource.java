package com.xq;

import com.xq.apitest.pojo.SensorReading;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

public class RandomSource extends RichSourceFunction<SensorReading> {
    private boolean isCancel = false;

    @Override
    public void run(SourceContext<SensorReading> sourceContext) throws Exception {
        Random random = new Random();

        Map<Integer, Double> map = new HashMap(10);
        for (int i = 0; i < 10; i++) {
            map.put(i, 60 + random.nextGaussian() * 20);
        }

        while (!isCancel) {
            Iterator<Map.Entry<Integer, Double>> iterator = map.entrySet().iterator();
            for (; iterator.hasNext(); ) {
                Map.Entry<Integer, Double> next = iterator.next();
                map.put(next.getKey(), next.getValue() + random.nextGaussian());
            }
            final long currentTimeMillis = System.currentTimeMillis();
            map.forEach((k, v) -> {
                System.out.println(k+":"+v);
                sourceContext.collect(new SensorReading(k.toString(), currentTimeMillis, v));
            });
            // 间隔200ms
            Thread.sleep(200);
        }
    }

    @Override
    public void cancel() {
        isCancel = true;
    }
}
