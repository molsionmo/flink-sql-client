package com.github.mxb.flink.sql.exception;

import org.apache.flink.util.FlinkException;

/**
 * Class which indicates a problem when deploying a Flink resource.
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
