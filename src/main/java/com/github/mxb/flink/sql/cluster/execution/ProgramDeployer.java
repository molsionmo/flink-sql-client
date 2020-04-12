package com.github.mxb.flink.sql.cluster.execution;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.local.result.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * job-depolyer;The helper class to deploy a table program  on the cluster.
 *
 * @author moxianbin
 */
public class ProgramDeployer<C> extends org.apache.flink.table.client.gateway.local.ProgramDeployer implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ProgramDeployer.class);

    private final ExecutionContext<C> context;
    private final JobGraph jobGraph;
    private final String jobName;
    private final Result<C> result;
    private final boolean isDetached;
    private final BlockingQueue<JobExecutionResult> executionResultBucket;

    /**
     * Deploys a table program on the cluster.
     *
     * @param context    context with deployment information
     * @param jobName    job name of the Flink job to be submitted
     * @param jobGraph   Flink job graph
     * @param result     result that receives information about the target cluster
     * @param isDetached block for a job execution result from the cluster
     */
    public ProgramDeployer(
            ExecutionContext<C> context,
            String jobName,
            JobGraph jobGraph,
            Result<C> result,
            Boolean isDetached) {
        super(null, jobName, jobGraph, result, isDetached);
        this.context = context;
        this.jobGraph = jobGraph;
        this.jobName = jobName;
        this.result = result;
        this.isDetached = Optional.ofNullable(isDetached).orElse(true);
        executionResultBucket = new LinkedBlockingDeque<>(1);
    }

    @Override
    public void run() {
        LOG.info("Submitting job {} for query {}`", jobGraph.getJobID(), jobName);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Submitting job {} with the following environment: \n{}",
                    jobGraph.getJobID(), context.getMergedEnvironment());
        }
        deployJob(context, jobGraph, result);
    }

    @Override
    public JobExecutionResult fetchExecutionResult() {
        return executionResultBucket.poll();
    }

    /**
     * Deploys a job. Depending on the deployment creates a new job cluster. It saves the cluster id in
     * the result and blocks until job completion.
     */
    private <T> void deployJob(ExecutionContext<T> context, JobGraph jobGraph, Result<T> result) {
        // create or retrieve cluster and deploy job
        try (final ClusterDescriptor<T> clusterDescriptor = context.createClusterDescriptor()) {
            try {
                // new cluster
                if (context.getClusterId() == null) {
                    deployJobOnNewCluster(clusterDescriptor, jobGraph, result, context.getClassLoader());
                }
                // reuse existing cluster
                else {
                    deployJobOnExistingCluster(context.getClusterId(), clusterDescriptor, jobGraph, result);
                }
            } catch (Exception e) {
                throw new SqlExecutionException("", e);
            }
        } catch (SqlExecutionException e) {
            throw e;
        } catch (Exception e) {
            throw new SqlExecutionException("Could not locate a cluster.", e);
        }
    }

    private <T> void deployJobOnNewCluster(
            ClusterDescriptor<T> clusterDescriptor,
            JobGraph jobGraph,
            Result<T> result,
            ClassLoader classLoader) throws Exception {
        ClusterClient<T> clusterClient = null;
        try {
            // deploy job cluster with job attached
            clusterClient = clusterDescriptor.deployJobCluster(context.getClusterSpec(), jobGraph, false);
            // save information about the new cluster
            result.setClusterInformation(clusterClient.getClusterId(), clusterClient.getWebInterfaceURL());
            // get result
            if (!isDetached) {
                // we need to hard cast for now
                final JobExecutionResult jobResult = ((RestClusterClient<T>) clusterClient)
                        .requestJobResult(jobGraph.getJobID())
                        .get()
                        .toJobExecutionResult(context.getClassLoader()); // throws exception if job fails
                executionResultBucket.add(jobResult);
            }
        } finally {
            try {
                if (clusterClient != null) {
                    clusterClient.shutdown();
                }
            } catch (Exception e) {
                // ignore
            }
        }
    }

    private <T> void deployJobOnExistingCluster(
            T clusterId,
            ClusterDescriptor<T> clusterDescriptor,
            JobGraph jobGraph,
            Result<T> result) throws Exception {
        ClusterClient<T> clusterClient = null;
        try {
            // retrieve existing cluster
            clusterClient = clusterDescriptor.retrieve(clusterId);
            String webInterfaceUrl;
            // retrieving the web interface URL might fail on legacy pre-FLIP-6 code paths
            // TODO remove this once we drop support for legacy deployment code
            try {
                webInterfaceUrl = clusterClient.getWebInterfaceURL();
            } catch (Exception e) {
                webInterfaceUrl = "N/A";
            }
            // save the cluster information
            result.setClusterInformation(clusterClient.getClusterId(), webInterfaceUrl);
            // submit job (and get result)
            if (!isDetached) {
                clusterClient.setDetached(false);
                final JobExecutionResult jobResult = clusterClient
                        .submitJob(jobGraph, context.getClassLoader())
                        .getJobExecutionResult(); // throws exception if job fails
                executionResultBucket.add(jobResult);
            } else {
                clusterClient.setDetached(true);
                clusterClient.submitJob(jobGraph, context.getClassLoader());
            }
        } finally {
            try {
                if (clusterClient != null) {
                    clusterClient.shutdown();
                }
            } catch (Exception e) {
                // ignore
            }
        }
    }
}
