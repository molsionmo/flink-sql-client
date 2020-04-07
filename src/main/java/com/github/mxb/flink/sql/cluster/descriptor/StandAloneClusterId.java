package com.github.mxb.flink.sql.cluster.descriptor;

/**
 *  stand alone集群的id
 * @author mxb
 */
public final class StandAloneClusterId {
    /**
     * rest address; eg: cdh01.name01.com
     */
    private final String restAddress;

    private final int port;

    public StandAloneClusterId(String restAddress, int port) {
        this.restAddress = restAddress;
        this.port = port;
    }

    public String getRestAddress() {
        return restAddress;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "StandAloneClusterId{" + "restAddress='" + restAddress + '\'' + ", port=" + port + '}';
    }
}
