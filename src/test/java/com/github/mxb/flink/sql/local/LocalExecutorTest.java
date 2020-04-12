package com.github.mxb.flink.sql.local;

import com.github.mxb.flink.sql.exception.FlinkClientTimeoutException;
import com.github.mxb.flink.sql.factory.ExecutorFactory;
import com.github.mxb.flink.sql.cluster.minicluster.MiniClusterResource;
import com.github.mxb.flink.sql.cluster.model.run.JobRunConfig;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.flink.sql.parser.error.SqlParseException;
import org.apache.flink.table.client.gateway.ProgramTargetDescriptor;
import org.apache.flink.util.FlinkException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class LocalExecutorTest {

    LocalExecutor localExecutor;

    private int numberTaskManagers = 4;
    private int numberSlotsPerTaskManager = 4;
    private String dependencyJarsDir = "./dependencies";

    @Before
    public void setUp() throws Exception {
        MiniClusterResource.MiniClusterResourceConfiguration miniClusterResourceConfiguration =
                new MiniClusterResource.MiniClusterResourceConfiguration(numberTaskManagers, numberSlotsPerTaskManager);

        localExecutor = ExecutorFactory.createExecutor(miniClusterResourceConfiguration);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void kafkaToMysql() throws IOException, FlinkException, FlinkClientTimeoutException, SqlParseException, InterruptedException {
        JobRunConfig jobRunConfig1 = JobRunConfig.builder().jobName("jobName")
                .checkpointInterval(1000).defaultParallelism(1).build();

        String dependencyJarDir =dependencyJarsDir;
        String sql = Files.toString(new File(getClass().getClassLoader().getResource("sqlsumbit/kafkaToMysql.sql").getPath()), Charsets.UTF_8);

        ProgramTargetDescriptor targetDescriptor = localExecutor.executeSqlJob(jobRunConfig1, dependencyJarDir, sql);

        Thread.sleep(200_000);
    }

    @Test
    public void groupByTest() throws IOException, SqlParseException, InterruptedException {
        JobRunConfig jobRunConfig1 = JobRunConfig.builder().jobName("jobName")
                .checkpointInterval(1000).defaultParallelism(1).build();

        String dependencyJarDir =dependencyJarsDir;
        String sql = Files.toString(new File(getClass().getClassLoader().getResource("sqlsumbit/pvuv_kafkaToMysql_groupBy.sql").getPath()), Charsets.UTF_8);

        ProgramTargetDescriptor targetDescriptor = localExecutor.executeSqlJob(jobRunConfig1, dependencyJarDir, sql);

        Thread.sleep(200_000);
    }
}