package com.github.mxb.flink.sql.local.minicluster;

/**
 * MiniClusterManage control the number of miniClusterResource.
 * It can get the only one MiniCluster in the environment
 *
 * @author moxianbin
 * @since 2020/4/13 14:09
 */
public class MiniClusterManage {

    private static MiniClusterResource miniClusterResource;

    static {
        miniClusterResource = MiniClusterResource.getDefaultInstance();
    }

    public static MiniClusterResource getMiniClusterResourceInstance(){
        return miniClusterResource;
    }

}
