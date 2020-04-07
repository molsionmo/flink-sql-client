package com.github.mxb.flink.sql.exception;

import org.apache.flink.util.FlinkException;

/**
 * Exception which indicates that a cluster resource could not be retrieved.
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
