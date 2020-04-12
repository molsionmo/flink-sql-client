package com.github.mxb.flink.sql.cluster;

/**
 * @description     cluster status
 * @auther          moxianbin
 * @create          2020-04-11 20:44:56
 */
public interface ClusterStatus<S> {
    S getStatus();

    boolean isRunning();
}
