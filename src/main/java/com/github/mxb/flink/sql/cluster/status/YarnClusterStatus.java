package com.github.mxb.flink.sql.cluster.status;

import com.github.mxb.flink.sql.cluster.ClusterStatus;
import lombok.Builder;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

/**
 * Flink on Yarn cluster status
 */
@Builder
public class YarnClusterStatus implements ClusterStatus<ApplicationReport> {

    private ApplicationReport applicationReport;

    @Override
    public ApplicationReport getStatus() {
        return applicationReport;
    }

    @Override
    public boolean isRunning() {
        return FinalApplicationStatus.UNDEFINED.equals(applicationReport.getFinalApplicationStatus());
    }
}
