package com.github.mxb.flink.sql.cluster.resource;

public enum AuthType {

    /**
     * 不使用认证
     */
    NONE,

    SIMPLE,

    KERBEROS,

    TOKEN,
}
