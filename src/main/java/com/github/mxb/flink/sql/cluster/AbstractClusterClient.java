package com.github.mxb.flink.sql.cluster;

import com.github.mxb.flink.sql.exception.FlinkClientTimeoutException;
import com.github.mxb.flink.sql.cluster.execution.ExecutionContext;
import com.github.mxb.flink.sql.cluster.execution.ProgramDeployer;
import com.github.mxb.flink.sql.cluster.model.run.JobConfig;
import com.github.mxb.flink.sql.cluster.model.run.JobRunConfig;
import com.github.mxb.flink.sql.cluster.model.run.JobRunType;
import com.github.mxb.flink.sql.parser.SqlNodeInfo;
import com.github.mxb.flink.sql.util.ExceptionUtils;
import com.github.mxb.flink.sql.parser.FlinkSqlParserUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.error.SqlParseException;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.config.entries.FunctionEntry;
import org.apache.flink.table.client.gateway.ProgramTargetDescriptor;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.local.result.BasicResult;
import org.apache.flink.table.descriptors.FunctionDescriptor;
import org.apache.flink.table.functions.FunctionService;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.util.FlinkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 *@description     AbstractClusterClient
 *@auther          moxianbin
 *@create          2020-04-11 19:12:45
 */
public abstract class AbstractClusterClient<T> implements ClusterClient<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractClusterClient.class);

    private static final AtomicInteger TMP_VIEW_SEQUENCE_ID = new AtomicInteger(0);

    private static final String EXECUTION_TYPE_KEY = "{EXECUTION_TYPE}";

    protected static String defaultEnvSchema =
            "execution:\n" +
                    "  type: " + EXECUTION_TYPE_KEY + "\n" +
                    "  time-characteristic: event-time\n" +
                    "  periodic-watermarks-interval: 200\n" +
                    "  result-mode: table\n" +
                    "  max-table-result-rows: 1000000\n" +
                    "  parallelism: 1\n" +
                    "  max-parallelism: 128\n" +
                    "  min-idle-state-retention: 0\n" +
                    "  max-idle-state-retention: 0\n" +
                    "  restart-strategy:\n" +
                    "    type: fixed-delay\n" +
                    "    attempts: 3\n" +
                    "    delay: 10000\n" +
                    "deployment:\n" +
                    "  response-timeout: 10000\n";

    /**
     * execute sql job with config and commandLine
     *
     * @param jobConfig
     * @param dependencyJars
     * @param sql
     * @param flinkConfig
     * @param commandLineOptions
     * @param commandLines
     * @return
     * @throws SqlExecutionException
     * @throws SqlParseException
     */
    protected ProgramTargetDescriptor executeSqlJob(JobConfig jobConfig, List<File> dependencyJars, String sql,
                                                    Configuration flinkConfig, Options commandLineOptions, List<CustomCommandLine<?>> commandLines) throws SqlExecutionException, SqlParseException {

        final Environment sessionEnv = new Environment();
        final SessionContext session = new SessionContext(jobConfig.getJobRunConfig().getJobName(), sessionEnv);

        List<SqlNodeInfo> sqlNodeList = FlinkSqlParserUtil.parseSqlContext(sql);
        List<SqlNodeInfo> insertSqlNodes = sqlNodeList.stream().filter(node -> SqlKind.INSERT.lowerName.equalsIgnoreCase(node.getSqlNode().getKind().lowerName)).collect(Collectors.toList());
        final ExecutionContext<?> context = getExecutionContext(jobConfig.getJobRunConfig(), dependencyJars, flinkConfig, commandLineOptions, commandLines, session);

        //registerDDL
        registerDDL(sqlNodeList, context);

        List<String> insertSqlList = insertSqlNodes.stream().map(SqlNodeInfo::getOriginSql).collect(Collectors.toList());
        return executeUpdate(context, insertSqlList, jobConfig);
    }

    protected ExecutionContext<?> getExecutionContext(JobRunConfig jobRunConfig, List<File> dependencyJars, Configuration flinkConfig, Options commandLineOptions, List<CustomCommandLine<?>> commandLines, SessionContext session) {
        //init env and dependencyJars
        Environment environment = getEnvironment(jobRunConfig);
        //init executionContext
        return createExecutionContext(session, environment, dependencyJars, jobRunConfig, flinkConfig, commandLineOptions, commandLines);
    }

    /**
     * get executionContext
     *
     * @param session
     * @param environment
     * @param dependencies
     * @param jobRunConfig
     * @return
     */
    protected ExecutionContext<?> createExecutionContext(SessionContext session, Environment environment, List<File> dependencies, JobRunConfig jobRunConfig,
                                                         Configuration flinkConfig, Options commandLineOptions, List<CustomCommandLine<?>> commandLines) {
        ExecutionContext executionContext;
        try {
            executionContext = new ExecutionContext<>(environment, session, dependencies,
                    flinkConfig, commandLineOptions, commandLines, jobRunConfig);
        } catch (Throwable t) {
            // catch everything such that a configuration does not crash the com.ppmon.bigdata.flink.sql.client.executor
            throw new SqlExecutionException("Could not create execution context.", t);
        }

        return executionContext;
    }

    protected void registerDDL(List<SqlNodeInfo> sqlNodeInfoList, ExecutionContext<?> context) {
        //TODO 按照 function -> createTable -> createView 进行排序

        sqlNodeInfoList.forEach(sqlNode -> {
            if (sqlNode.getSqlNode() instanceof SqlCreateTable) {
                this.createTable(context, sqlNode);
            }
            if (sqlNode.getSqlNode() instanceof SqlCreateView) {
                this.createView(context, sqlNode);
            }
        });
    }

    protected Environment getEnvironment(JobRunConfig jobRunConfig) {
        String envSchema = defaultEnvSchema;

        if (null != jobRunConfig.getJobRunType()) {
            envSchema = envSchema.replace(EXECUTION_TYPE_KEY, jobRunConfig.getJobRunType().name().toLowerCase());
        } else {
            envSchema = envSchema.replace(EXECUTION_TYPE_KEY, JobRunType.STREAMING.name().toLowerCase());
        }


        if (StringUtils.isNotBlank(jobRunConfig.getRestoreSavePointPath())) {
            envSchema += "\ndeployment:\n" + "  s: " + jobRunConfig.getRestoreSavePointPath();

            if (Objects.nonNull(jobRunConfig.getIsAllowNonRestoredState()) && jobRunConfig.getIsAllowNonRestoredState()) {
                envSchema += "\n  n: true";
            }
        }

        Environment environment;
        try {
            environment = Environment.parse(envSchema);
        } catch (IOException e) {
            throw new SqlClientException("Could not read default environment", e);
        }

        return environment;
    }

    protected <C> ProgramTargetDescriptor executeUpdate(ExecutionContext<C> context, List<String> statements, JobConfig jobConfig) {
        final ExecutionContext.EnvironmentInstance envInst = context.createEnvironmentInstance();

        ExecutionConfig.GlobalJobParameters globalJobParameters = envInst.getExecutionConfig().getGlobalJobParameters();
        if (globalJobParameters == null) {
            globalJobParameters = new Configuration();
        }

        Configuration configuration = (Configuration) globalJobParameters;
        jobConfig.getJobParameter().forEach(configuration::setString);
        envInst.getExecutionConfig().setGlobalJobParameters(configuration);

        statements.forEach(statement -> applyUpdate(context, envInst.getTableEnvironment(), statement));

        // create job graph with dependencies
        final String jobName = jobConfig.getJobRunConfig().getJobName();
        final JobGraph jobGraph;
        try {
            // createJobGraph requires an optimization step that might reference UDFs during code compilation
            jobGraph = context.wrapClassLoader(() -> {
                return envInst.createJobGraph(jobName);
            });
        } catch (Throwable t) {
            // catch everything such that the statement does not crash the com.ppmon.bigdata.flink.sql.client.executor
            throw new SqlExecutionException("Invalid SQL statement.", t);
        }

        // create execution
        final BasicResult<C> result = new BasicResult<>();
        boolean isDetached = Optional.ofNullable(jobConfig.getJobRunConfig().getIsDetached()).orElse(true);
        final ProgramDeployer<C> deployer = new ProgramDeployer<>(context, jobName, jobGraph, result, isDetached);

        // blocking deployment
        deployer.run();

        return ProgramTargetDescriptor.of(
                result.getClusterId(),
                jobGraph.getJobID(),
                result.getWebInterfaceUrl());
    }

    /**
     * Applies the given update statement to the given table environment with query configuration.
     */
    private <C> void applyUpdate(ExecutionContext<C> context, TableEnvironment tableEnv, String updateStatement) {
        // parse and validate statement
        try {
            // update statement requires an optimization step that might reference UDFs during code compilation
            context.wrapClassLoader(() -> {
                tableEnv.sqlUpdate(updateStatement);
                return null;
            });
        } catch (Throwable t) {
            // catch everything such that the statement does not crash the com.ppmon.bigdata.flink.sql.client.executor
            throw new SqlExecutionException("Invalid SQL update statement.", t);
        }
    }

    protected void createFunction(ExecutionContext<?> context, SqlNodeInfo sqlNode) throws SqlExecutionException {
        final ExecutionContext.EnvironmentInstance envInst = context.createEnvironmentInstance();
        TableEnvironment tEnv = envInst.getTableEnvironment();

        //TODO

    }

    /**
     * Create user defined function.
     */
    private static UserDefinedFunction createUserDefinedFunction(ClassLoader classLoader, String funcName, String funcDef) {
        Map<String, Object> config = new HashMap<>();
        config.put("name", funcName);
        config.put("from", "class");
        config.put("class", funcDef);

        final FunctionDescriptor desc = FunctionEntry.create(config).getDescriptor();

        return FunctionService.createFunction(desc, classLoader, false);
    }

    protected void createTable(ExecutionContext<?> context, SqlNodeInfo sqlNode) throws SqlExecutionException {
        final ExecutionContext.EnvironmentInstance envInst = context.createEnvironmentInstance();
        TableEnvironment tEnv = envInst.getTableEnvironment();

        try {
            tEnv.sqlUpdate(sqlNode.getOriginSql());
        } catch (Exception e) {
            throw new SqlExecutionException("Could not create a table from ddl: " + sqlNode.getOriginSql(), e);
        }
    }

    protected void createView(ExecutionContext<?> context, SqlNodeInfo sqlNode) throws SqlExecutionException {
        final ExecutionContext.EnvironmentInstance envInst = context.createEnvironmentInstance();
        TableEnvironment tEnv = envInst.getTableEnvironment();

        try {
            String subQuery = ((SqlCreateView) (sqlNode.getSqlNode())).getQuery().toString();
            Table view = tEnv.sqlQuery(subQuery);
            tEnv.registerTable(((SqlCreateView) sqlNode.getSqlNode()).getViewName().toString(), view);
        } catch (Exception e) {
            throw new SqlExecutionException("Could not create a view from ddl: " + sqlNode.getOriginSql(), e);
        }
    }

    protected static Predicate<Throwable> isConnectionProblemException(){
        return throwable  ->
                ExceptionUtils.findThrowable(throwable, java.net.ConnectException.class).isPresent() ||
                        ExceptionUtils.findThrowable(throwable, java.net.SocketTimeoutException.class).isPresent() ||
                        ExceptionUtils.findThrowable(throwable, org.apache.flink.shaded.netty4.io.netty.channel.ConnectTimeoutException.class).isPresent() ||
                        ExceptionUtils.findThrowable(throwable, java.io.IOException.class).isPresent();
    }

    protected void reThrowException(Exception e) throws FlinkClientTimeoutException, FlinkException {
        if ( isConnectionProblemException().test(e) ){
            throw new FlinkClientTimeoutException(e);
        }

        throw new FlinkException(e);
    }
}
