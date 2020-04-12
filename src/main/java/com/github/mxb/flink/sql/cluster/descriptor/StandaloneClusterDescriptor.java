package com.github.mxb.flink.sql.cluster.descriptor;

import com.github.mxb.flink.sql.cluster.ClusterDescriptor;
import com.github.mxb.flink.sql.cluster.ClusterSpecification;
import com.github.mxb.flink.sql.cluster.ClusterStatus;
import com.github.mxb.flink.sql.exception.ClusterDeploymentException;
import com.github.mxb.flink.sql.exception.ClusterKillException;
import com.github.mxb.flink.sql.exception.ClusterRetrieveException;
import com.github.mxb.flink.sql.cluster.ClusterClient;
import com.github.mxb.flink.sql.cluster.client.JobManagerClient;
import com.github.mxb.flink.sql.cluster.status.StandaloneClusterStatus;
import com.github.mxb.flink.sql.cluster.status.standalone.StandaloneClusterReport;
import com.github.mxb.flink.sql.util.OkHttpUtils;
import okhttp3.Response;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *@description     StandaloneClusterDescriptor
 *@auther          moxianbin
 *@create          2020-04-11 19:11:45
 */
public class StandaloneClusterDescriptor implements ClusterDescriptor<StandAloneClusterId, StandaloneClusterReport> {

    private final static Logger log = LoggerFactory.getLogger(StandaloneClusterDescriptor.class);

    @Override
    public String getResourceDescription() {
        return null;
    }

    @Override
    public ClusterStatus<StandaloneClusterReport> retrieveClusterStatus(StandAloneClusterId clusterId) throws IOException, YarnException {
        String configUrl = "http://" + clusterId.getRestAddress() + ":" + clusterId.getPort() + "/config";

        Response response = OkHttpUtils.get(configUrl);
        StandaloneClusterReport report = new StandaloneClusterReport();
        if (response.isSuccessful()){
            report.setStatus(StandaloneClusterReport.Status.RUNNING);
        } else {
            report.setStatus(StandaloneClusterReport.Status.UNKOWN);
        }

        return StandaloneClusterStatus.builder().
                standaloneClusterReport(report).
                build();
    }

    @Override
    public ClusterClient<StandAloneClusterId> retrieve(StandAloneClusterId clusterId) throws ClusterRetrieveException {
        Configuration flinkConfiguration = new Configuration();
        flinkConfiguration.setString(RestOptions.ADDRESS, clusterId.getRestAddress());
        flinkConfiguration.setInteger(RestOptions.PORT, clusterId.getPort());

        return new JobManagerClient<>(flinkConfiguration);
    }

    @Override
    public ClusterClient<StandAloneClusterId> deploySessionCluster(ClusterSpecification clusterSpecification) throws ClusterDeploymentException {
        throw new UnsupportedOperationException("暂不支持该操作");
    }

    @Override
    public void killSession(StandAloneClusterId clusterId) throws ClusterKillException {
        throw new UnsupportedOperationException("暂不支持该操作");
    }

    @Override
    public void close() throws Exception {
        // TODO 完善整个资源关闭链
    }
}
