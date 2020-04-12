package com.github.mxb.flink.sql.exception;

import org.apache.flink.util.FlinkException;

/**
 * @description     Class which indicates a problem when deploying a Flink resource.
 * @auther          moxianbin
 * @create          2020-04-12 11:34:53
 */
public class ClusterDeploymentException extends FlinkException {

    private static final long serialVersionUID = -4327724979766139207L;

    public ClusterDeploymentException(String message) {
        super(message);
    }

    public ClusterDeploymentException(Throwable cause) {
        super(cause);
    }

    public ClusterDeploymentException(String message, Throwable cause) {
        super(message, cause);
    }
}
