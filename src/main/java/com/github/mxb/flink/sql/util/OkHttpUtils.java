package com.github.mxb.flink.sql.util;

import com.alibaba.fastjson.JSONObject;
import okhttp3.*;
import org.apache.commons.lang3.Validate;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 提供常用Http请求的封装
 *
 * @author moxianbin
 */
public final class OkHttpUtils {

    public static final int DEFAULT_CONNECTION_TIMEOUT = 30;

    public static final int DEFAULT_READ_TIMEOUT = 60;

    public static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

    // 默认client
    private static final OkHttpClient client = new OkHttpClient.Builder()
            .connectTimeout(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.SECONDS)
            .readTimeout(DEFAULT_READ_TIMEOUT, TimeUnit.SECONDS)
            .build();

    private OkHttpUtils() {

    }

    public static <T> T get(String url, Class<T> cls) throws IOException {
        return getType(get(client, url), cls);
    }

    /**
     * get请求
     *
     * @param url 请求url
     * @return 响应对象
     * @throws IOException io异常
     */
    public static Response get(String url) throws IOException {
        return get(client, url);
    }

    /**
     * get请求，默认连接超时
     *
     * @param url                 请求url
     * @param readTimeoutInMillis 读超时时间，单位微秒
     * @return 响应对象，需要关闭
     * @throws IOException io异常
     */
    public static Response get(String url, int readTimeoutInMillis) throws IOException {
        Validate.notEmpty(url, "url can not be null.");
        Validate.isTrue(readTimeoutInMillis > 0, "readTimeoutInMillis must be > 0");

        OkHttpClient newClient = new OkHttpClient.Builder()
                .connectTimeout(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.SECONDS)
                .readTimeout(readTimeoutInMillis, TimeUnit.MILLISECONDS)
                .build();

        return get(newClient, url);
    }

    /**
     * post表单
     *
     * @param url    请求地址
     * @param params 表单数据
     * @return 响应对象
     * @throws IOException io异常
     */
    public static Response post(String url, Map<String, String> params) throws IOException {
        Validate.notNull(params, "form body can not be null.");
        return postParam(client, url, params);
    }

    /**
     * post表单
     *
     * @param url                 请求地址
     * @param params              表单数据
     * @param readTimeoutInMillis 读超时时间，单位微秒
     * @return 响应对象
     * @throws IOException io异常
     */
    public static Response post(String url, Map<String, String> params, int readTimeoutInMillis) throws IOException {
        Validate.notNull(params, "form body can not be null.");
        Validate.isTrue(readTimeoutInMillis > 0, "readTimeoutInMillis must be > 0");

        OkHttpClient newClient = new OkHttpClient.Builder()
                .connectTimeout(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.SECONDS)
                .readTimeout(readTimeoutInMillis, TimeUnit.MILLISECONDS)
                .build();

        return postParam(newClient, url, params);
    }

    /**
     * post json
     *
     * @param url  请求地址
     * @param json json
     * @return 返回
     * @throws IOException io异常
     */
    public static Response post(String url, String json) throws IOException {
        return postJson(client, url, json);
    }

    /**
     * post json
     *
     * @param url                 请求地址
     * @param json                json
     * @param readTimeoutInMillis 读超时，单位微秒
     * @return 响应对象
     * @throws IOException io异常
     */
    public static Response post(String url, String json, int readTimeoutInMillis) throws IOException {
        Validate.notEmpty(url, "url can not be null.");
        Validate.isTrue(readTimeoutInMillis > 0, "readTimeoutInMillis must be > 0");

        OkHttpClient newClient = new OkHttpClient.Builder()
                .connectTimeout(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.SECONDS)
                .readTimeout(readTimeoutInMillis, TimeUnit.MILLISECONDS)
                .build();

        return postJson(newClient, url, json);
    }

    private static Response get(OkHttpClient client, String url) throws IOException {
        Request request = new Request.Builder()
                .url(url)
                .build();
        return client.newCall(request).execute();
    }

    private static Response postParam(OkHttpClient httpClient, String url, Map<String, String> params) throws IOException {
        FormBody.Builder builder = new FormBody.Builder();
        params.forEach(builder::add);
        FormBody body = builder.build();

        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .build();

        return httpClient.newCall(request).execute();
    }

    private static Response postJson(OkHttpClient httpClient, String url, String json) throws IOException {
        RequestBody body = RequestBody.create(JSON, json);
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .build();
        return httpClient.newCall(request).execute();
    }

    private static <T> T getType(Response response, Class<T> cls) throws IOException {
        if (response.isSuccessful()) {
            if (response.body() == null) {
                return null;
            }
            if (cls == String.class) {
                return cls.cast(response.body().string());
            }

            String content = response.body().string();
            return JSONObject.parseObject(content, cls);
        }

        return null;
    }
}
