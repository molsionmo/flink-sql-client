# flink sql client

基于flink-1.9.1,支持flink在不同集群模式下的sql任务提交，区别于flink官方提供的sql client模块,官方提供的是需要单独执行的服务，这里提供的是一个sdk,可以无缝接入自己的流计算平台.

## quick start

### standalone模式
* 启动flink standalone集群,rest端口为8081
* 运行com.github.mxb.flink.sql.cluster.StandaloneClusterTest中的kafkaToMysqlTest测试用例

### flink on yarn模式
* 启动flink on yarn集群,获得applicationId
* 运行com.github.mxb.flink.sql.cluster.YarnClusterClientTest中的kafkaToMysqlTest测试用例

### 本地Minicluster模式
* 运行com.github.mxb.flink.sql.executor.LocalExecutorTest里面的测试用例

## sdk quick start

* 项目引入flink-sql-client
```
<dependency>
    <groupId>com.github.mxb</groupId>
    <artifactId>flink-sql-client</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

* 实例化取出clusterClient
```java
public class Test{
    
    public ClusterClient getClusterClient(){
        ResourceInfo standaloneResourceInfo = new ResourceInfo();
        standaloneResourceInfo.setResourceType(ResourceType.STANDALONE);
        
        ClusterDescriptor clusterDescriptor = ClusterDescriptorFactory.createClusterDescriptor(standaloneResourceInfo);
        StandAloneClusterId standAloneClusterId = new StandAloneClusterId("127.0.0.1", 8081);
        ClusterClient clusterClient = clusterDescriptor.retrieve(standAloneClusterId);
    }

    public String executeSqlJob(String sql, List<File> dependencyJars){
        ClusterClient clusterClient = getClusterClient();
        JobRunConfig jobRunConfig = JobRunConfig.builder()
                        .jobName(getTestJobName())
                        .defaultParallelism(1)
                        .sourceParallelism(1)
                        .checkpointInterval(60_000L).build();
        ProgramTargetDescriptor programTargetDescriptor = clusterClient.executeSqlJob(jobRunConfig, dependencyJars, sql);
        return programTargetDescriptor.getJobId();
    }   

}
```

## 问题处理

* 执行groupByTest用例时会出现InvalidClassException异常，local class incompatible serialVersionUID;解决:在flink-parent中修改对应的类并重新引入
```scala
@scala.SerialVersionUID(value = 1)
abstract class ProcessFunctionWithCleanupState
```
