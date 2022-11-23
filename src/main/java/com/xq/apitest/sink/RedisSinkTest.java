package com.xq.apitest.sink;

import com.xq.apitest.pojo.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.net.URL;

public class RedisSinkTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        URL resource = RedisSinkTest.class.getResource("/sensor.txt");
        DataStreamSource<String> inputStream = env.readTextFile(resource.getPath());
        SingleOutputStreamOperator<SensorReading> sensorReadingStream = inputStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0].trim(), Long.parseLong(split[1].trim()), Double.parseDouble(split[2].trim()));
            }
        });

        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379)
                .build();

        sensorReadingStream.addSink(new RedisSink<>(jedisPoolConfig, new RedisMapper<SensorReading>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, "sensor_temp");
            }

            @Override
            public String getKeyFromData(SensorReading sensorReading) {
                return sensorReading.getTemperature().toString();
            }

            @Override
            public String getValueFromData(SensorReading sensorReading) {
                return sensorReading.getId();
            }
        }));
    }
}
