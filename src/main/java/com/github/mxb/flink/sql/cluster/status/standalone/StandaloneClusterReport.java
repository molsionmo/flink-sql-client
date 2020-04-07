package com.github.mxb.flink.sql.cluster.status.standalone;

/**
 *  StandaloneClusterReport
 */
public class StandaloneClusterReport {

    private Status status;

    public StandaloneClusterReport() {
    }

    public StandaloneClusterReport(Status status) {
        this.status = status;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public enum Status{
        /**
         * 运行中
         */
        RUNNING,
        /**
         * 未知
         */
        UNKOWN
    }
}
