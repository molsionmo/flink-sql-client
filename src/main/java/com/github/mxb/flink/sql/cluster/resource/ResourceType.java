package com.github.mxb.flink.sql.cluster.resource;

public enum ResourceType {

    /**
     * stand alone集群方式
     */
    STANDALONE(true),

    YARN(true),

    K8S(false);

    /**
     * 是否支持
     */
    private boolean supported;

    ResourceType(boolean supported) {
        this.supported = supported;
    }

    public boolean isSupported() {
        return supported;
    }
}
