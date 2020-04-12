package com.github.mxb.flink.sql.exception;

/**
 * @description     ClusterKillException
 * @auther          moxianbin
 * @create          2020-04-12 11:35:05
 */
public class ClusterKillException extends Exception {

    private static final long serialVersionUID = 9179106523018018512L;

    public ClusterKillException(String message) {
        super(message);
    }

    public ClusterKillException(String message, Throwable cause) {
        super(message, cause);
    }

    public ClusterKillException(Throwable cause) {
        super(cause);
    }
}
