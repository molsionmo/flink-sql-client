package com.github.mxb.flink.sql.cluster;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.github.mxb.flink.sql.cluster.descriptor.StandAloneClusterId;
import com.github.mxb.flink.sql.cluster.resource.ResourceInfo;
import com.github.mxb.flink.sql.cluster.resource.ResourceType;
import com.github.mxb.flink.sql.exception.FlinkClientTimeoutException;
import com.github.mxb.flink.sql.factory.ClusterDescriptorFactory;
import com.github.mxb.flink.sql.model.run.JobRunConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.sql.parser.error.SqlParseException;
import org.apache.flink.table.client.gateway.ProgramTargetDescriptor;
import org.apache.flink.util.FlinkException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;

@Slf4j
public class StandaloneClusterTest {

    private String jmAddress = "http://127.0.0.1:8081";

    private ResourceInfo resourceInfo;
    private ClusterDescriptor clusterDescriptor;
    private ClusterClient clusterClient;
    private String dependencyJarsDir = "./dependencies";
    private JobRunConfig jobRunConfig;

    @Before
    public void setUp() throws Exception {
        resourceInfo = new ResourceInfo();
        resourceInfo.setResourceType(ResourceType.STANDALONE);
        resourceInfo.setJmAddress(jmAddress);

        resourceInfo.setResourceType(ResourceType.STANDALONE);
        resourceInfo.setJmAddress(jmAddress);
        clusterDescriptor = ClusterDescriptorFactory.createClusterDescriptor(
                resourceInfo
        );

        StandAloneClusterId standAloneClusterId = new StandAloneClusterId("127.0.0.1", 8081);
        clusterClient = clusterDescriptor.retrieve(standAloneClusterId);
        jobRunConfig = JobRunConfig.builder()
                .jobName(getTestJobName())
                .defaultParallelism(1)
                .sourceParallelism(1)
                .checkpointInterval(60_000L).build();
    }

    @After
    public void tearDown() throws Exception {
    }

    //TODO dependencyJarDir依赖
    @Test
    public void kafkaToMysql() throws IOException, FlinkException, FlinkClientTimeoutException, SqlParseException {
        JobRunConfig jobRunConfig = JobRunConfig.builder()
                .jobName(getTestJobName())
                .defaultParallelism(1)
                .sourceParallelism(1)
                .checkpointInterval(60_000L).build();
        String dependencyJarDir = dependencyJarsDir;

        String sql = Files.toString(new File(getClass().getClassLoader().getResource("sqlsumbit/kafkaToMysql.sql").getPath()), Charsets.UTF_8);
        ProgramTargetDescriptor programTargetDescriptor = clusterClient.executeSqlJob(jobRunConfig,new ArrayList<>(),sql);

        log.info("jobId: {}",programTargetDescriptor.getJobId());
    }


    // TODO  InvalidClassException异常 local class incompatible serialVersionUID
    @Test
    public void groupByTest() throws IOException, FlinkException, FlinkClientTimeoutException, SqlParseException {
        String dependencyJarDir = dependencyJarsDir;

        String sql = Files.toString(new File(getClass().getClassLoader().getResource("sqlsumbit/pvuv_kafkaToMysql_groupBy.sql").getPath()), Charsets.UTF_8);

        ProgramTargetDescriptor programTargetDescriptor = clusterClient.executeSqlJob(jobRunConfig,dependencyJarDir,sql);

        log.info("jobId: {}",programTargetDescriptor.getJobId());
    }

    @Test
    public void createViewTest() throws IOException, FlinkException, FlinkClientTimeoutException, SqlParseException {
        String dependencyJarDir = dependencyJarsDir;

        String sql = Files.toString(new File(getClass().getClassLoader().getResource("sqlsumbit/kafkaToMysqlCreateView.sql").getPath()), Charsets.UTF_8);

        ProgramTargetDescriptor programTargetDescriptor = clusterClient.executeSqlJob(jobRunConfig,dependencyJarDir,sql);
        log.info("jobId: {}",programTargetDescriptor.getJobId());
    }

    @Test
    public void cancelJobWithoutSavepoint() throws FlinkException, FlinkClientTimeoutException {
        String savepointPath = clusterClient.cancelJob("487b9e1c8c388aa3c951da771505791a", "file:///Users/mac/opt/flink-1.9.1/savepoint");
        log.info("{}", savepointPath);
    }

    @Test
    public void stopJobWithoutSavepoint() throws FlinkException, FlinkClientTimeoutException {
        String savepointPath = clusterClient.stopJob("487b9e1c8c388aa3c951da771505791a", "file:///Users/mac/opt/flink-1.9.1/savepoint");
        log.info("{}", savepointPath);
    }

    private String getTestJobName(){
        return "jobNameTest_"+ UUID.randomUUID().toString();
    }
}