package com.github.mxb.flink.sql.cluster.descriptor;

import com.github.mxb.flink.sql.cluster.ClusterDescriptor;
import com.github.mxb.flink.sql.cluster.ClusterSpecification;
import com.github.mxb.flink.sql.cluster.ClusterStatus;
import com.github.mxb.flink.sql.exception.ClusterDeploymentException;
import com.github.mxb.flink.sql.exception.ClusterKillException;
import com.github.mxb.flink.sql.exception.ClusterRetrieveException;
import com.github.mxb.flink.sql.cluster.ClusterClient;
import com.github.mxb.flink.sql.cluster.client.JobManagerClusterClient;
import com.github.mxb.flink.sql.cluster.status.YarnClusterStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author mxb
 */
public class YarnClusterDescriptor implements ClusterDescriptor<ApplicationId, ApplicationReport> {

    private final static Logger log = LoggerFactory.getLogger(YarnClusterDescriptor.class);

    private YarnConfiguration yarnConfiguration;

    private YarnClient yarnClient;

    private Configuration flinkConfiguration;

    public YarnClusterDescriptor(Configuration flinkConfiguration,
                                 YarnConfiguration yarnConfiguration,
                                 YarnClient yarnClient) {

        this.yarnConfiguration = Preconditions.checkNotNull(yarnConfiguration);
        this.yarnClient = Preconditions.checkNotNull(yarnClient);

        this.flinkConfiguration = Preconditions.checkNotNull(flinkConfiguration);
    }



    @Override
    public String getResourceDescription() {
        return null;
    }

    @Override
    public ClusterStatus<ApplicationReport> retrieveClusterStatus(ApplicationId clusterId) throws IOException, YarnException {
        final ApplicationReport appReport = yarnClient.getApplicationReport(clusterId);

        return YarnClusterStatus.builder()
                .applicationReport(appReport)
                .build();
    }

    @Override
    public ClusterClient<ApplicationId> retrieve(ApplicationId clusterId) throws ClusterRetrieveException {
        try {
            final ApplicationReport appReport = yarnClient.getApplicationReport(clusterId);

            if (appReport.getFinalApplicationStatus() != FinalApplicationStatus.UNDEFINED) {
                // Flink cluster is not running anymore
                log.error("The application {} doesn't run anymore. It has previously completed with final status: {}",
                        clusterId, appReport.getFinalApplicationStatus());
                throw new RuntimeException("The Yarn application " + clusterId + " doesn't run anymore.");
            }

            final String host = appReport.getHost();
            final int rpcPort = appReport.getRpcPort();

            log.info("Found application JobManager host name '{}' and port '{}' from supplied application id '{}'",
                    host, rpcPort, clusterId);

            flinkConfiguration.setString(JobManagerOptions.ADDRESS, host);
            flinkConfiguration.setInteger(JobManagerOptions.PORT, rpcPort);

            flinkConfiguration.setString(RestOptions.ADDRESS, host);
            flinkConfiguration.setInteger(RestOptions.PORT, rpcPort);

            return createYarnClusterClient(
                    flinkConfiguration
            );

        } catch (Exception e) {
            throw new ClusterRetrieveException("Could not retrieve Yarn cluster", e);
        }
    }

    protected ClusterClient<ApplicationId> createYarnClusterClient(Configuration flinkConfiguration){
        return new JobManagerClusterClient<>(flinkConfiguration);
    }

    @Override
    public ClusterClient<ApplicationId> deploySessionCluster(ClusterSpecification clusterSpecification) throws ClusterDeploymentException {
        throw new UnsupportedOperationException("暂不支持该操作");
    }

    @Override
    public void killSession(ApplicationId clusterId) throws ClusterKillException {
        throw new UnsupportedOperationException("暂不支持该操作");
    }

    @Override
    public void close() throws Exception {
        // TODO 完善整个资源关闭链
    }
}
