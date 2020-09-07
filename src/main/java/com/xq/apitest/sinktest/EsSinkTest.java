package com.xq.apitest.sinktest;

import com.xq.apitest.pojo.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.index.VersionType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EsSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从文件读取数据
        DataStreamSource<String> inputStream = env.readTextFile("D:\\code\\FlinkTutorial_1.10\\src\\main\\resources\\sensor.txt");
        // 1. 基本转换操作：map成样例类类型
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map((MapFunction<String, SensorReading>) value -> {
            String[] split = value.split(",");
            return new SensorReading(split[0].trim(), Long.parseLong(split[1].trim()), Double.parseDouble(split[2].trim()));
        });

        // 定义httphosts
        List<HttpHost> httpHosts = new ArrayList();
        httpHosts.add( new HttpHost("localhost", 9200) );

        // 定义es sink function
        ElasticsearchSinkFunction<SensorReading> esSinkFun =  new ElasticsearchSinkFunction<SensorReading>() {

            @Override
            public void process(SensorReading sensorReading, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                Map dataSource = new HashMap<String, String>();
                dataSource.put("sensor_id", sensorReading.getId());
                dataSource.put("temp", sensorReading.getTemperature().toString());
                dataSource.put("ts", sensorReading.getTemperature().toString());

                List<IndexRequest> list = new ArrayList<>();
                // 创建index request
                IndexRequest indexRequest = Requests.indexRequest()
                        .index("sensor")
                        .type("data")
                        .id("ddddd")
                        //可以设置版本类型与版本,对于更新保序数据很有用
                        .versionType(VersionType.EXTERNAL).version(234234L)
                        .source(dataSource);
                list.add(indexRequest);
                IndexRequest[] arr = new IndexRequest[list.size()];
                // 使用RequestIndexer发送http请求
                requestIndexer.add(list.toArray(arr));
//                requestIndexer.add(indexRequest);

                System.out.println("data " + sensorReading + " saved successfully");
            }
        };

        dataStream.addSink(new ElasticsearchSink.Builder<SensorReading>(httpHosts,esSinkFun).build());

        env.execute("test es sink job");
    }
}

