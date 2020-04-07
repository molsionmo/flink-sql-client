package com.github.mxb.flink.sql.exception;

/**
 * represent flink client request timeout exception
 * eg : java.net.ConnectException;java.net.SocketTimeoutException;io.netty.channel.ConnectTimeoutException
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
