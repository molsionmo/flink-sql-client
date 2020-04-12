package com.github.mxb.flink.sql.cluster;

import com.github.mxb.flink.sql.cluster.descriptor.StandaloneClusterDescriptor;
import com.github.mxb.flink.sql.cluster.resource.AuthType;
import com.github.mxb.flink.sql.cluster.resource.ResourceInfo;
import com.github.mxb.flink.sql.cluster.resource.ResourceType;
import com.github.mxb.flink.sql.cluster.status.YarnClusterStatus;
import com.github.mxb.flink.sql.exception.FlinkClientTimeoutException;
import com.github.mxb.flink.sql.factory.ClusterDescriptorFactory;
import com.github.mxb.flink.sql.model.monitor.JobRunStatusEnum;
import com.github.mxb.flink.sql.model.run.JobRunConfig;
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
import java.util.Objects;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

@Slf4j
public class YarnClusterClientTest {

    //跨域请求; name01.sabigdata.com是另一个集群,且kdc为name01.sabigdata.com
    private String host = "http://quickstart.cloudera:8088";
    private String krb5Realm = "REAL.IO";
    private String krb5Kdc = "kdc-test.REAL.io";
    private String principle = "kafka_hdfs@REAL.IO";
//    private String keyTabPath = Objects.requireNonNull(getClass().getClassLoader().getResource("./krb/kafka_hdfs.keytab")).getPath();
//    private String krb5ConfFilePath = Objects.requireNonNull(getClass().getClassLoader().getResource("./krb/krb5.conf")).getPath();

    private String applicationIdStr = "application_1586665211655_0002";
    private ResourceInfo resourceInfo;
    private ClusterDescriptor clusterDescriptor;
    private ClusterClient clusterClient;
    private ApplicationId applicationId;

    private String dependencyJarsDir = "./dependencies";
    @Before
    public void setUp() throws Exception {
        resourceInfo = new ResourceInfo();
        resourceInfo.setResourceId("clusterResourceId");
        resourceInfo.setResourceType(ResourceType.YARN);
        resourceInfo.setYarnRmAddress(host);
        resourceInfo.setAuthType(AuthType.NONE);
//        resourceInfo.setKrb5Kdc(krb5Kdc);
//        resourceInfo.setKrb5Realm(krb5Realm);
//        resourceInfo.setKeytabPath(keyTabPath);
//        resourceInfo.setKeytabPrinciple(principle);
//        resourceInfo.setKrb5ConfFilePath(krb5ConfFilePath);

        clusterDescriptor = ClusterDescriptorFactory.createClusterDescriptor(
                resourceInfo,
                new Configuration()
        );
        applicationId = ConverterUtils.toApplicationId(applicationIdStr);
        clusterClient = clusterDescriptor.retrieve(applicationId);
    }

    @Test
    public void createClusterDescriptor() throws IOException {
        resourceInfo.setResourceType(ResourceType.K8S);
        assertThatThrownBy(()->ClusterDescriptorFactory.createClusterDescriptor(resourceInfo)).hasMessage("暂不支持的资源类型:"+resourceInfo.getResourceType().name());

        resourceInfo.setResourceType(ResourceType.STANDALONE);
        resourceInfo.setJmAddress(host);

        clusterDescriptor = ClusterDescriptorFactory.createClusterDescriptor(resourceInfo);
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
        clusterClient.getJobsRunStatus(Lists.newArrayList("7107643deaa489ad0a50b3611bef1c7f"))
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
    public void executeSqlJob() throws FlinkException, FlinkClientTimeoutException, SqlParseException {
        // executeSqlJob
        JobRunConfig jobRunConfig = JobRunConfig.builder()
                .jobName(getTestJobName())
                .defaultParallelism(1)
                .sourceParallelism(1)
                .checkpointInterval(60_000L).build();
        String dependencyJarDir = dependencyJarsDir;
        String sql = "CREATE TABLE t(key VARBINARY, msg VARBINARY, `topic` VARCHAR, `partition` INT, `offset` bigint)with(type = 'KAFKA011PLUS',topic = 'abc',`bootstrap.servers` = 'bigdata85:9092,bigdata86:9092,bigdata87:9092',`group.id` = 'test-group',startupMode = 'EARLIEST');" +
                "CREATE TABLE MyResult(kkey varchar,msg varchar,PRIMARY KEY(kkey))" +
                "  WITH(type='JDBC',drivername='com.mysql.jdbc.Driver'," +
                "dburl='jdbc:mysql://192.168.102.70:3306/test?charset=utf8&autoReconnect=true&characterEncoding=utf8'," +
                "username='root',password='ppIT@jiedai.1234',tablename='mytb'," +
                "`batch.interval`='5');" +
                "insert into MyResult(kkey,msg) SELECT CAST (t.msg AS VARCHAR) as tmsg, CAST (t.key AS VARCHAR) as ttkey FROM t where CAST (t.msg AS VARCHAR) like '%我们%';";
        ProgramTargetDescriptor targetDescriptor = clusterClient.executeSqlJob(jobRunConfig,dependencyJarDir,sql);

        log.info("jobId:{}", targetDescriptor.getJobId());
    }

    @Test
    public void joinJdbc() throws Exception {
        // executeSqlJob1
        JobRunConfig jobRunConfig1 = JobRunConfig.builder().jobName(getTestJobName())
                .defaultParallelism(1)
                .build();

        String dependencyJarDir = dependencyJarsDir;

        String sql = Files.toString(new File(getClass().getClassLoader().getResource("join-jdbc.sql").getPath()), Charsets.UTF_8);

        ProgramTargetDescriptor targetDescriptor = clusterClient.executeSqlJob(jobRunConfig1, dependencyJarDir, sql);
        log.info("jobId:{}", targetDescriptor.getJobId());
    }

    @Test
    public void joinHbase() throws Exception {
        // executeSqlJob1
        JobRunConfig jobRunConfig1 = JobRunConfig.builder().jobName(getTestJobName())
                .defaultParallelism(1)
                .build();

        String dependencyJarDir = dependencyJarsDir;
        String sql = Files.toString(new File(getClass().getClassLoader().getResource("join-hbase.sql").getPath()), com.google.common.base.Charsets.UTF_8);

        ProgramTargetDescriptor targetDescriptor = clusterClient.executeSqlJob(jobRunConfig1, dependencyJarDir, sql);
        log.info("jobId:{}", targetDescriptor.getJobId());
    }

    @Test
    public void moreSource() throws FlinkException, FlinkClientTimeoutException, SqlParseException {
        JobRunConfig jobRunConfig = JobRunConfig.builder()
                .jobName(getTestJobName())
                .defaultParallelism(1)
                .sourceParallelism(1)
                .checkpointInterval(60_000L).build();
        String dependencyJarDir = dependencyJarsDir;

        String sql = "CREATE TABLE userInfo(\n" +
                "\t`userId` int,\n" +
                "    `age` int ,\n" +
                "    `name` VARCHAR,\n" +
                "    `loginTime` TIMESTAMP,\n" +
                "    watermark for `loginTime` as withoffset(`loginTime`, 1000)"+
                "    ) \n" +
                "    with (\n" +
                "        type = 'KAFKA011PLUS', \n" +
                "        topic = 'userInfo', \n" +
                "        `bootstrap.servers` = 'bigdata85:9092,bigdata86:9092,bigdata87:9092,',\n" +
                "        `group.id` = 'test-group', \n" +
                "        startupMode = 'EARLIEST', \n" +
                "        `kafka.schema`='json'\n" +
                "        );\n" +
                "\t\t\n" +
                "CREATE TABLE userInvest(\n" +
                "    `userId` int,\n" +
                "    `investAmount` BIGINT\n" +
                "    ) \n" +
                "    with (\n" +
                "        type = 'KAFKA011PLUS', \n" +
                "        topic = 'userInvest', \n" +
                "        `bootstrap.servers` = 'bigdata85:9092,bigdata86:9092,bigdata87:9092,',\n" +
                "        `group.id` = 'test-group', \n" +
                "        startupMode = 'EARLIEST', \n" +
                "        `kafka.schema`='json'\n" +
                "        );\n" +
                "\n" +
                "CREATE TABLE myResult(\n" +
                "\t`userId` int,\n" +
                "    `age` int ,\n" +
                "    `name` VARCHAR,\n" +
                "\t`investAmount` BIGINT\n" +
                "\t)\n" +
                "\twith (\n" +
                "\t\ttype='JDBC',\n" +
                "\t\tdrivername='com.mysql.jdbc.Driver',\n" +
                "\t\tusername='root',\n" +
                "\t\tdburl='jdbc:mysql://192.168.102.70:3306/test?charset=utf8&autoReconnect=true&characterEncoding=utf8',\n" +
                "\t\tpassword='ppIT@jiedai.1234',\n" +
                "\t\ttablename='userInvest',\n" +
                "\t\t`batch.interval`='5'\n" +
                "\t);\n" +
                "\t\n" +
                "insert into myResult(`userId`,`age`,`name`,`investAmount`)\n" +
                "select info.`userId`,info.`age`,info.`name`,invest.`investAmount`\n" +
                "from userInfo as info\n" +
                "join userInvest as invest\n" +
                "on info.`userId` = invest.`userId`";

        ProgramTargetDescriptor targetDescriptor = clusterClient.executeSqlJob(jobRunConfig,dependencyJarDir,sql);

        log.info("jobId:{}", targetDescriptor.getJobId());
    }

    @Test
    public void newKafkaSinkJdbc() throws SqlParseException, FlinkException, FlinkClientTimeoutException {
        // executeSqlJob
        JobRunConfig jobRunConfig = JobRunConfig.builder()
                .jobName(getTestJobName())
                .defaultParallelism(1)
                .sourceParallelism(1)
                .checkpointInterval(60_000L).build();

        String dependencyJarDir = dependencyJarsDir;
        String sql = "CREATE TABLE t(key VARCHAR,`user.name` VARCHAR ,`user.age` INT ,`grade` VARCHAR, `create_time` TIMESTAMP(3), `ctime` TIMESTAMP(3),`etime` TIMESTAMP(3), watermark for `etime` as withoffset(`etime`, 1000), PRIMARY KEY(key) ) with (type = 'KAFKA011PLUS', topic = 'axyzjson2source', `bootstrap.servers` = 'bigdata85:9092,bigdata86:9092,bigdata87:9092,', `group.id` = 'test-group', startupMode = 'EARLIEST', `kafka.schema`='json2');" +
                "CREATE TABLE kafka_sink(key VARCHAR, `user.sinkname` VARCHAR ,`user.sinkage` INT ,`user.sinkgrade` VARCHAR, `sinkctime` TIMESTAMP(3), PRIMARY KEY (key)) with (type = 'KAFKA011', topic = 'axyzjson2sink', `bootstrap.servers` = 'bigdata85:9092,bigdata86:9092,bigdata87:9092,', retries = '3');" +
                "INSERT INTO kafka_sink (key , `user.sinkname`  ,`user.sinkage`  ,`user.sinkgrade` , `sinkctime`)  select key ,`user.name`  ,`user.age`  ,`grade` ,TUMBLE_end(etime, INTERVAL '1' MINUTE)  from t GROUP BY TUMBLE(etime, INTERVAL '1' MINUTE),key ,`user.name`  ,`user.age`  ,`grade`";
        ProgramTargetDescriptor targetDescriptor = clusterClient.executeSqlJob(jobRunConfig,dependencyJarDir,sql);

        log.info("jobId:{}", targetDescriptor.getJobId());
    }

    @Test
    public void cancelJob() throws FlinkException, FlinkClientTimeoutException {
        String savepointPath = clusterClient.cancelJob("2d217ae5aea9e3c2e1d7ca646817b5d4","hdfs://nameservice1/flink/savepoint");
        log.info("savepointPath:{}",savepointPath);
    }

    @Test
    public void cdc_savepoint_restart() throws IOException, FlinkException, FlinkClientTimeoutException, SqlParseException {
        // executeSqlJob
        JobRunConfig jobRunConfig = JobRunConfig.builder()
                .jobName(getTestJobName())
                .defaultParallelism(1)
                .restoreSavePointPath("hdfs://nameservice1/flink/savepoint/default01/savepoint-23b9f8-9dcad109e17b")
                .isAllowNonRestoredState(true)
                .checkpointInterval(60_000L).build();

        String dependencyJarDir = dependencyJarsDir;

        String sql = Files.toString(new File(getClass().getClassLoader().getResource("./fix/cdc_savepoint.sql").getPath()), Charsets.UTF_8);
        ProgramTargetDescriptor targetDescriptor = clusterClient.executeSqlJob(jobRunConfig,dependencyJarDir,sql);
    }

    @Test
    public void cdc_savepoint_consistent() throws IOException, SqlParseException, InterruptedException, FlinkException, FlinkClientTimeoutException {
        // executeSqlJob
        JobRunConfig jobRunConfig = JobRunConfig.builder()
                .jobName(getTestJobName())
                .defaultParallelism(1)
                .checkpointInterval(60_000L).build();
        String dependencyJarDir = dependencyJarsDir;
        String sql = Files.toString(new File(getClass().getClassLoader().getResource("./fix/cdc_savepoint_consistent.sql").getPath()), Charsets.UTF_8);

        ProgramTargetDescriptor targetDescriptor = clusterClient.executeSqlJob(jobRunConfig,dependencyJarDir,sql);
    }

    private String getTestJobName(){
        return "jobNameTest_"+ UUID.randomUUID().toString();
    }
}
