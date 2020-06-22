package com.github.mxb.flink.sql.cluster;

import com.github.mxb.flink.sql.cluster.descriptor.StandaloneClusterDescriptor;
import com.github.mxb.flink.sql.cluster.resource.AuthType;
import com.github.mxb.flink.sql.cluster.resource.FlinkResourceInfo;
import com.github.mxb.flink.sql.cluster.resource.ResourceType;
import com.github.mxb.flink.sql.cluster.status.YarnClusterStatus;
import com.github.mxb.flink.sql.exception.FlinkClientTimeoutException;
import com.github.mxb.flink.sql.factory.ClusterDescriptorFactory;
import com.github.mxb.flink.sql.cluster.model.run.overview.JobRunStatusEnum;
import com.github.mxb.flink.sql.cluster.model.run.JobRunConfig;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.sql.parser.error.SqlParseException;
import org.apache.flink.table.client.gateway.ProgramTargetDescriptor;
import org.apache.flink.util.FlinkException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;

import org.assertj.core.api.AssertProvider;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

@Slf4j
public class YarnClusterClientTest {

    //跨域请求; name01.sabigdata.com是另一个集群,且kdc为name01.sabigdata.com
    private String host = "http://quickstart.cloudera:8088";
    private String krb5Realm = "REAL.IO";
    private String krb5Kdc = "kdc-test.REAL.io";
    private String principle = "hdfs@REAL.IO";
//    private String keyTabPath = Objects.requireNonNull(getClass().getClassLoader().getResource("./krb/hdfs.keytab")).getPath();
//    private String krb5ConfFilePath = Objects.requireNonNull(getClass().getClassLoader().getResource("./krb/krb5.conf")).getPath();

    private String applicationIdStr = "application_1586665211655_0002";
    private FlinkResourceInfo flinkResourceInfo;
    private ClusterDescriptor clusterDescriptor;
    private ClusterClient clusterClient;
    private ApplicationId applicationId;

    private String dependencyJarsDir = "./dependencies";
    @Before
    public void setUp() throws Exception {
        flinkResourceInfo = new FlinkResourceInfo();
        flinkResourceInfo.setResourceId("clusterResourceId");
        flinkResourceInfo.setResourceType(ResourceType.YARN);
        flinkResourceInfo.setYarnRmAddress(host);
        flinkResourceInfo.setAuthType(AuthType.NONE);
//        resourceInfo.setKrb5Kdc(krb5Kdc);
//        resourceInfo.setKrb5Realm(krb5Realm);
//        resourceInfo.setKeytabPath(keyTabPath);
//        resourceInfo.setKeytabPrinciple(principle);
//        resourceInfo.setKrb5ConfFilePath(krb5ConfFilePath);

        clusterDescriptor = ClusterDescriptorFactory.createClusterDescriptor(
                flinkResourceInfo,
                new Configuration()
        );
        applicationId = ConverterUtils.toApplicationId(applicationIdStr);
        clusterClient = clusterDescriptor.retrieve(applicationId);
    }

    @Test
    public void createClusterDescriptor() throws IOException {
        flinkResourceInfo.setResourceType(ResourceType.K8S);
        assertThatThrownBy(()->ClusterDescriptorFactory.createClusterDescriptor(flinkResourceInfo)).hasMessage("暂不支持的资源类型:"+ flinkResourceInfo.getResourceType().name());

        flinkResourceInfo.setResourceType(ResourceType.STANDALONE);
        flinkResourceInfo.setJmAddress(host);

        clusterDescriptor = ClusterDescriptorFactory.createClusterDescriptor(flinkResourceInfo);
        assertThat((AssertProvider<Boolean>) ()-> clusterDescriptor instanceof StandaloneClusterDescriptor);

    }

    @Test
    public void getYarnClusterStatus() throws IOException, YarnException {
        YarnClusterStatus flinkOnYarnClusterStatus = (YarnClusterStatus) clusterDescriptor.retrieveClusterStatus(applicationId);
        ApplicationReport applicationReport = flinkOnYarnClusterStatus.getStatus();
        log.info("{}", applicationReport.getYarnApplicationState());
    }

    @Test
    public void createYarnClusterDescriptor() throws IOException, FlinkException, FlinkClientTimeoutException {
        clusterClient.getJobStatus(Lists.newArrayList("7107643deaa489ad0a50b3611bef1c7f"))
                .forEach((k, v) -> System.out.println(k + "----" + v));

        Map<String, JobRunStatusEnum> jobRunStatusEnumMaps = clusterClient.getJobStatus(Lists.newArrayList("7107643deaa489ad0a50b3611bef1c7f"));
        jobRunStatusEnumMaps.forEach((k,v)->System.out.println(k + "----" + v.name()));
        jobRunStatusEnumMaps.forEach((k,v)->System.out.println(k + "----" + v.isGloballyTerminalState()));

    }

    @Test
    public void getOverviewUrl(){
        clusterClient.getJobsOverviewUrl(Lists.newArrayList("b93bd30671a740333c7a363b82694fb2"))
                .forEach((k, v)-> System.out.println(k + "----" + v));
    }

    @Test
    public void executeSqlJob() throws FlinkException, FlinkClientTimeoutException, SqlParseException, IOException {
        // executeSqlJob
        JobRunConfig jobRunConfig = JobRunConfig.builder()
                .jobName(getTestJobName())
                .defaultParallelism(1)
                .sourceParallelism(1)
                .checkpointInterval(60_000L).build();
        String dependencyJarDir = dependencyJarsDir;
        String sql = Files.toString(new File(getClass().getClassLoader().getResource("sqlsumbit/kafkaToMysql.sql").getPath()), Charsets.UTF_8);
        ProgramTargetDescriptor targetDescriptor = clusterClient.executeSqlJob(jobRunConfig,dependencyJarDir,sql);

        log.info("jobId:{}", targetDescriptor.getJobId());
    }

    @Test
    public void cancelJob() throws FlinkException, FlinkClientTimeoutException {
        String savepointPath = clusterClient.cancel("2d217ae5aea9e3c2e1d7ca646817b5d4","hdfs://nameservice1/flink/savepoint");
        log.info("savepointPath:{}",savepointPath);
    }

    private String getTestJobName(){
        return "jobNameTest_"+ UUID.randomUUID().toString();
    }
}
