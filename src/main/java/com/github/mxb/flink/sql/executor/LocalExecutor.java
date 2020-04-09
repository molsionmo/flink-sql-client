package com.github.mxb.flink.sql.executor;

import com.github.mxb.flink.sql.executor.local.LocalCustomCommandLine;
import com.github.mxb.flink.sql.executor.local.LocalExecutorConstants;
import com.github.mxb.flink.sql.minicluster.MiniClusterResource;
import com.github.mxb.flink.sql.model.monitor.JobMonitorOverview;
import com.github.mxb.flink.sql.model.run.JobConfig;
import com.github.mxb.flink.sql.model.run.JobRunConfig;
import com.github.mxb.flink.sql.model.run.ProgramResultDescriptor;
import com.github.mxb.flink.sql.parser.FlinkSqlParserUtil;
import com.github.mxb.flink.sql.parser.SqlNodeInfo;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.sql.parser.error.SqlParseException;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.ProgramTargetDescriptor;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.client.gateway.local.ResultStore;
import org.apache.flink.table.client.gateway.local.result.DynamicResult;
import org.apache.flink.table.client.gateway.local.result.MaterializedResult;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * LocalExecutor, create and start a miniCluster if do not have.
 * Execute job in local environment
 *
 * @author moxianbin
 * @since 2019/5/8 17:01
 */
public class LocalExecutor extends AbstractExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(LocalExecutor.class);

    private Configuration flinkConfig;
    private List<CustomCommandLine<?>> commandLines;
    private Options commandLineOptions;

    private ResultStore resultStore;

    private MiniClusterResource miniClusterResource;
    private ClusterClient<?> clusterClient;

    public LocalExecutor() {
    }

    public LocalExecutor(MiniClusterResource miniClusterResource) {

        this.clusterClient = miniClusterResource.getClusterClient();
        this.flinkConfig = clusterClient.getFlinkConfiguration();

        this.commandLines = Collections.singletonList(new LocalCustomCommandLine<>(clusterClient));
        this.commandLineOptions = collectCommandLineOptions(commandLines);

        resultStore = new ResultStore(this.flinkConfig);
        this.miniClusterResource = miniClusterResource;

    }

    public MiniClusterResource getMiniClusterResource() {
        return miniClusterResource;
    }

    @Override
    public Executor newInstance(Map<String, String> properties) throws Exception {

        int localTmNum = Integer.valueOf(properties.get(LocalExecutorConstants.LOCAL_TM_NUM_KEY));
        int localTmPerSlotsNum = Integer.valueOf(properties.get(LocalExecutorConstants.LOCAL_NUM_SLOTS_PER_TM_KEY));

        MiniClusterResource.MiniClusterResourceConfiguration miniClusterResourceConfiguration = new MiniClusterResource.MiniClusterResourceConfiguration(
                localTmNum, localTmPerSlotsNum
        );

        MiniClusterResource miniClusterResource = new MiniClusterResource(miniClusterResourceConfiguration);
        miniClusterResource.startCluster();

        return new LocalExecutor(miniClusterResource);
    }

    @Override
    public List<String> supportedProperties() {
        return new ArrayList<>();
    }

    @Override
    public List<String> requiredProperties() {
        return new ArrayList<>();
    }

    @Override
    public String cancelJob(String jobId, String savepointDir) throws FlinkException {
        String savepointPath = "";

        try {
            if (StringUtils.isNotBlank(savepointDir)) {
                savepointPath = this.clusterClient.cancelWithSavepoint(JobID.fromHexString(jobId), savepointDir);
            } else {
                this.clusterClient.cancel(JobID.fromHexString(jobId));
            }
        } catch (Exception e){
            throw new UndeclaredThrowableException(e);
        }

        return savepointPath;
    }

    @Override
    public String triggerSavepoint(String jobId, String savepointDir) throws FlinkException {

        CompletableFuture<String> completableFuture = this.clusterClient.triggerSavepoint(JobID.fromHexString(jobId), savepointDir);

        String savePointPath = "";
        try {
            savePointPath = completableFuture.get();
        } catch ( ExecutionException | InterruptedException e){
            throw new UndeclaredThrowableException(e);
        }

        return savePointPath;
    }

    @Override
    public ProgramTargetDescriptor executeSqlJob(JobRunConfig jobRunConfig, String dependencyJarDir, String sql) throws SqlExecutionException, SqlParseException {
        JobConfig jobConfig = new JobConfig(jobRunConfig, new HashMap<>());
        return executeSqlJob(jobConfig, dependencyJarDir, sql, flinkConfig, commandLineOptions, commandLines);
    }

    @Override
    public ProgramTargetDescriptor executeSqlJob(JobConfig jobConfig, String dependencyJarDir, String sql) throws SqlExecutionException, SqlParseException {
        return executeSqlJob(jobConfig, dependencyJarDir, sql, flinkConfig, commandLineOptions, commandLines);
    }

    @Override
    public ProgramResultDescriptor executeQueryInternal(JobRunConfig jobRunConfig, String dependencyJarDir, String sql) throws SqlExecutionException, SqlParseException {
        final Environment sessionEnv = new Environment();
        final SessionContext session = new SessionContext(jobRunConfig.getJobName(), sessionEnv);

        final ExecutionContext<?> context = getExecutionContext(jobRunConfig, dependencyJarDir, flinkConfig, commandLineOptions, commandLines, session);
        final ExecutionContext.EnvironmentInstance envInst = context.getEnvironmentInstance();

        List<SqlNodeInfo> sqlNodeList = FlinkSqlParserUtil.parseSqlContext(sql);
        //registerDDL
        registerDDL(sqlNodeList, context);

        List<SqlNodeInfo> selectSqlNodes = sqlNodeList.stream().filter(node -> SqlKind.SELECT.lowerName.equalsIgnoreCase(node.getSqlNode().getKind().lowerName)).collect(Collectors.toList());
        List<SqlNodeInfo> insertSqlNodes = sqlNodeList.stream().filter(node -> SqlKind.INSERT.lowerName.equalsIgnoreCase(node.getSqlNode().getKind().lowerName)).collect(Collectors.toList());

        executeValidate(selectSqlNodes, insertSqlNodes);

        String selectSql = "";
        if (!selectSqlNodes.isEmpty()) {
            selectSql = selectSqlNodes.stream().findFirst().get().getOriginSql();
        } else {
            SqlInsert sqlInsert = (SqlInsert) insertSqlNodes.stream().findFirst().get().getSqlNode();
            selectSql = sqlInsert.getSource().toString();
        }

        // create table
        final Table table = context.getEnvironmentInstance().getTableEnvironment().sqlQuery(selectSql);

        // initialize result
        final DynamicResult result = resultStore.createResult(
                context.getMergedEnvironment(),
                removeTimeAttributes(table.getSchema()),
                envInst.getExecutionConfig());

        final JobGraph jobGraph;
        try {
            // createJobGraph requires an optimization step that might reference UDFs during code compilation
            jobGraph = context.wrapClassLoader(() -> {
                envInst.getTableEnvironment().registerTableSink(jobRunConfig.getJobName(), result.getTableSink());
                table.insertInto(
                        envInst.getQueryConfig(),
                        EnvironmentSettings.DEFAULT_BUILTIN_CATALOG,
                        EnvironmentSettings.DEFAULT_BUILTIN_DATABASE,
                        jobRunConfig.getJobName());
                return envInst.createJobGraph(jobRunConfig.getJobName());
            });
        } catch (Throwable t) {
            // the result needs to be closed as long as. it not stored in the result store
            result.close();
            // catch everything such that the query does not crash the executor
            throw new SqlExecutionException("Invalid SQL query.", t);
        }

        // store the result with a unique id (the job id for now)
        final String jobId = jobGraph.getJobID().toString();
        resultStore.storeResult(jobId, result);

        // create execution
        boolean isDetached = Optional.ofNullable(jobRunConfig.getIsDetached()).orElse(false);
        final ProgramDeployer deployThread = new ProgramDeployer<>(
                context, jobRunConfig.getJobName(), jobGraph, result, isDetached);

        // start result retrieval
        result.startRetrieval(deployThread);

        // loop get the result rows
        List<Row> resultRows = new ArrayList<>();
        if (!isDetached) {
            resultRows = retrieveTableResult(jobId);
        }

        return ProgramResultDescriptor.builder()
                .jobId(jobId)
                .isMaterialized(result.isMaterialized())
                .resultRows(resultRows).build();
    }

    @Override
    public ProgramResultDescriptor retrieveResult(String jobId) {
        List<Row> resultRows = retrieveTableResult(jobId);

        DynamicResult dynamicResult = resultStore.getResult(jobId);

        return ProgramResultDescriptor.builder()
                .jobId(jobId)
                .isMaterialized(dynamicResult.isMaterialized())
                .resultRows(resultRows)
                .build();
    }

    @Override
    public Map<String, String> getJobsRunStatus(List<String> jobIds) {
        return null;
    }

    @Override
    public Map<String, JobMonitorOverview> getJobsOverview(List<String> jobIds) {
        return null;
    }

    @Override
    public void start() throws SqlExecutionException {

    }

    private static Options collectCommandLineOptions(List<CustomCommandLine<?>> commandLines) {
        final Options customOptions = new Options();
        for (CustomCommandLine<?> customCommandLine : commandLines) {
            customCommandLine.addRunOptions(customOptions);
        }
        return CliFrontendParser.mergeOptions(
                CliFrontendParser.getRunCommandOptions(),
                customOptions);
    }

    private static TableSchema removeTimeAttributes(TableSchema schema) {
        final TableSchema.Builder builder = TableSchema.builder();
        for (int i = 0; i < schema.getFieldCount(); i++) {
            final DataType dataType = schema.getFieldDataTypes()[i];
            final DataType convertedType = DataTypeUtils.replaceLogicalType(
                    dataType,
                    LogicalTypeUtils.removeTimeAttributes(dataType.getLogicalType()));
            builder.field(schema.getFieldNames()[i], convertedType);
        }
        return builder.build();
    }

    private List<Row> retrieveTableResult(String jobId) {

        final List<Row> actualResults = new ArrayList<>();
        while (true) {
            final TypedResult<Integer> result = snapshotResult(jobId, 2);
            if (result.getType() == TypedResult.ResultType.PAYLOAD) {
                actualResults.clear();
                IntStream.rangeClosed(1, result.getPayload()).forEach((page) -> {
                    for (Row row : this.retrieveResultPage(jobId, page)) {
                        actualResults.add(row);
                    }
                });
            } else if (result.getType() == TypedResult.ResultType.EOS) {
                break;
            }
        }

        return actualResults;
    }

    private List<Row> retrieveResultPage(String resultId, int page) throws SqlExecutionException {
        final DynamicResult<?> result = resultStore.getResult(resultId);
        if (result == null) {
            throw new SqlExecutionException("Could not find a result with result identifier '" + resultId + "'.");
        }
        if (!result.isMaterialized()) {
            throw new SqlExecutionException("Invalid result retrieval mode.");
        }
        return ((MaterializedResult<?>) result).retrievePage(page);
    }

    public TypedResult<Integer> snapshotResult(String resultId, int pageSize) throws SqlExecutionException {
        final DynamicResult<?> result = resultStore.getResult(resultId);
        if (result == null) {
            throw new SqlExecutionException("Could not find a result with result identifier '" + resultId + "'.");
        }
        if (!result.isMaterialized()) {
            throw new SqlExecutionException("Invalid result retrieval mode.");
        }
        return ((MaterializedResult<?>) result).snapshot(pageSize);
    }

    private void executeValidate(List<SqlNodeInfo> selectSqlNodes, List<SqlNodeInfo> insertSqlNodes) {

        if (selectSqlNodes.isEmpty() && insertSqlNodes.isEmpty()) {
            throw new SqlExecutionException("execute do not support empty insert or select");
        }

        if (!selectSqlNodes.isEmpty() && !insertSqlNodes.isEmpty()) {
            throw new SqlExecutionException("execute do not support insert and select both run");
        }
    }
}
