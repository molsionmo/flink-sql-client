package com.github.mxb.flink.sql.exception;

/**
 * flink client request timeout exception
 * eg : java.net.ConnectException;java.net.SocketTimeoutException;io.netty.channel.ConnectTimeoutException
 * @description     represent flink client request timeout exception
 * @auther          moxianbin
 * @create          2020-04-12 11:35:25
 */
public class FlinkClientTimeoutException extends Exception{
    private final static long serialVersionUID = 1L;

    public FlinkClientTimeoutException(String message) {
        super(message);
    }

    public FlinkClientTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

    public FlinkClientTimeoutException(Throwable cause) {
        super(cause);
    }
}
