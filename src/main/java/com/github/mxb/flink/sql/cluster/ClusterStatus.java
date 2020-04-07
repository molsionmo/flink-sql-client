package com.github.mxb.flink.sql.cluster;

/**
 * cluster status
 */
public interface ClusterStatus<S> {
    S getStatus();

    boolean isRunning();
}
