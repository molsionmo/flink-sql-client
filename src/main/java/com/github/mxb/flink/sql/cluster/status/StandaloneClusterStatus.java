package com.github.mxb.flink.sql.cluster.status;

import com.github.mxb.flink.sql.cluster.ClusterStatus;
import com.github.mxb.flink.sql.cluster.status.standalone.StandaloneClusterReport;
import lombok.Builder;

/**
 *
 */
@Builder
public class StandaloneClusterStatus implements ClusterStatus<StandaloneClusterReport> {

    private StandaloneClusterReport standaloneClusterReport;

    public StandaloneClusterStatus(StandaloneClusterReport standaloneClusterReport) {
        this.standaloneClusterReport = standaloneClusterReport;
    }

    @Override
    public StandaloneClusterReport getStatus() {
        return standaloneClusterReport;
    }

    @Override
    public boolean isRunning() {
        return standaloneClusterReport.getStatus().equals(StandaloneClusterReport.Status.RUNNING);
    }

}
