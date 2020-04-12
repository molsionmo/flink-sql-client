package com.github.mxb.flink.sql.exception;

import org.apache.flink.util.FlinkException;

/**
 * @description     Exception which indicates that a cluster resource could not be retrieved.
 * @auther          moxianbin
 * @create          2020-04-12 11:35:13
 */
public class ClusterRetrieveException extends FlinkException {

    private static final long serialVersionUID = 7718062507419172318L;

    public ClusterRetrieveException(String message) {
        super(message);
    }

    public ClusterRetrieveException(Throwable cause) {
        super(cause);
    }

    public ClusterRetrieveException(String message, Throwable cause) {
        super(message, cause);
    }
}
