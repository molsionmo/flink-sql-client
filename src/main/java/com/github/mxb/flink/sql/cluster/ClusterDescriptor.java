package com.github.mxb.flink.sql.cluster;

import com.github.mxb.flink.sql.exception.ClusterDeploymentException;
import com.github.mxb.flink.sql.exception.ClusterKillException;
import com.github.mxb.flink.sql.exception.ClusterRetrieveException;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

/**
 * A descriptor to deploy a cluster resource(eg YARN or K8S)
 *
 * @param <T> type of the resource id
 * @param <S> type of the cluster status
 */
public interface ClusterDescriptor<T,S> extends AutoCloseable {

    /**
     * Returns a String containing details about the cluster (NodeManagers, available memory, ...).
     */
    String getResourceDescription();

    /**
     * Retrieves an existing Flink Cluster Status
     *
     * @param clusterId
     * @return
     * @throws IOException
     * @throws YarnException
     */
    ClusterStatus<S> retrieveClusterStatus(T clusterId) throws IOException, YarnException;

    /**
     * Retrieves an existing Flink Cluster Resource.
     *
     * @param clusterId The unique identifier of the running cluster resource
     * @return client for the cluster resource
     * @throws ClusterRetrieveException if the cluster client could not be retrieved
     */
    ClusterClient<T> retrieve(T clusterId) throws ClusterRetrieveException;

    /**
     * Triggers deployment of a cluster resource.
     *
     * @param clusterSpecification resource specification defining the cluster to deploy
     * @return client for the cluster resource
     * @throws ClusterDeploymentException if the cluster could not be deployed
     */
    ClusterClient<T> deploySessionCluster(ClusterSpecification clusterSpecification) throws ClusterDeploymentException;

    /**
     * Terminates the cluster resource with the given cluster id.
     *
     * @param clusterId identifying the cluster resource to shut down
     * @throws ClusterKillException if the cluster session could not be killed
     */
    void killSession(T clusterId) throws ClusterKillException;
}
