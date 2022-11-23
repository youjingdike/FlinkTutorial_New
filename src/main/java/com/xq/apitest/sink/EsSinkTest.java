package com.xq.apitest.sink;

import com.xq.apitest.pojo.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch6.RestClientFactory;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.index.VersionType;
import sun.security.jgss.GSSManagerImpl;

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

        ElasticsearchSink.Builder<SensorReading> builder = new ElasticsearchSink.Builder<>(httpHosts, esSinkFun);
        builder.setRestClientFactory(new RestClientFactory() {
            @Override
            public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {

                restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        Lookup<AuthSchemeProvider> authSchemeRegistry = RegistryBuilder.<AuthSchemeProvider>create().
                                register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true)).build();

                        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                //        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("", ""));
                        GSSManagerImpl gssManager = new GSSManagerImpl();
//                        Krb5InitCredential krb5InitCredential = Krb5InitCredential.
//                        GSSCredential gssCredential = new GSSCredentialImpl();
//                        credentialsProvider.setCredentials(new AuthScope(null, -1, null), new KerberosCredentials(gssCredential));

                        httpClientBuilder.setDefaultAuthSchemeRegistry(authSchemeRegistry);
                        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        return httpClientBuilder;
                    }
                });
            }
        });
        dataStream.addSink(builder.build());

        env.execute("test es sink job");
    }
}

class SinkEsFun extends RichSinkFunction<String> {

    @Override
    public void open(Configuration parameters) throws Exception {
//        System.setProperty("java.security.krb5.conf", "");
//        System.setProperty("es.security.indication", "true");
//
//        Header[] defaultHeaders = {};
//        HttpHost httpHost = new HttpHost("localhost",9200,"http");
//        RestClientBuilder builder = RestClient.builder(httpHost);
//        builder.setDefaultHeaders(defaultHeaders);
//
//        Lookup<AuthSchemeProvider> authSchemeRegistry = RegistryBuilder.<AuthSchemeProvider>create().
//                register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true)).build();
//
//        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
////        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("", ""));
//        GSSManagerImpl gssManager = new GSSManagerImpl();
//        Krb5InitCredential krb5InitCredential = Krb5InitCredential.
//        GSSCredential gssCredential = new GSSCredentialImpl();
//        credentialsProvider.setCredentials(new AuthScope(null, -1, null), new KerberosCredentials(gssCredential));
//
//        builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
//            @Override
//            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
//                httpClientBuilder.setDefaultAuthSchemeRegistry(authSchemeRegistry);
//                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
//                return httpClientBuilder;
//            }
//        });
//
////        RestClient client = builder.build();
//        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(builder);


    }
}

