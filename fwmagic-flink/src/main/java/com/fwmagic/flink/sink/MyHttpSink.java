package com.fwmagic.flink.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.http.Consts;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 自定义http sink
 */
public class MyHttpSink extends RichSinkFunction<String> {

    private static Logger logger = LoggerFactory.getLogger(MyHttpSink.class);

    private static final String URL = "http://localhost:9999/consume/consumerFromPost";

    private CloseableHttpClient httpClient;

    @Override
    public void open(Configuration parameters) {
        httpClient = HttpClients.custom()
                .setKeepAliveStrategy(DefaultConnectionKeepAliveStrategy.INSTANCE)
                .build();
    }

    @Override
    public void invoke(String value, Context context) {
        System.out.println(Thread.currentThread().getId() + "    invoke run:" + value);
        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("json", value));
        UrlEncodedFormEntity entity = new UrlEncodedFormEntity(params, Consts.UTF_8);

        HttpPost httpPost = new HttpPost(URL);
        httpPost.setEntity(entity);

        //设置连接超时时间（单位毫秒）和 请求获取数据的超时时间（单位毫秒）
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(5000)
                .setSocketTimeout(5000)
                .build();
        httpPost.setConfig(requestConfig);
        try {
            CloseableHttpResponse response = httpClient.execute(httpPost);
            if (response != null && response.getStatusLine() != null && response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                logger.info("成功发送的数据：{}", value);
            } else {
                logger.error("未成功发送的数据：{}", value);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("请求接口失败！", e);
        }
    }

    @Override
    public void close() throws Exception {
        httpClient.close();
    }

}
