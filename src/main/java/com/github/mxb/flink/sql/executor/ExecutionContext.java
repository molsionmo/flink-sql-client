package com.github.mxb.flink.sql.executor;

import com.github.mxb.flink.sql.model.run.JobRunConfig;
import com.github.mxb.flink.sql.util.JarUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.cli.RunOptions;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.plugin.TemporaryClassLoaderContext;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.plan.FlinkPlan;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.functions.*;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.config.entries.*;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.factories.*;
import org.apache.flink.table.planner.delegation.ExecutorBase;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.util.FlinkException;

import java.io.File;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.function.Supplier;

/**
 * <p>Context for executing table programs. This class caches everything that can be cached across
 * multiple queries as long as the session context does not change. This must be thread-safe as
 * it might be reused across different query submissions.</p>
 *
 * @param <T> cluster id
 * @author moxianbin on 2019/4/24 11:36
 */
public class ExecutionContext<T> {
    private final SessionContext sessionContext;
    private final Environment mergedEnv;
    private final List<File> dependencies;
    private final ClassLoader classLoader;
    private final Map<String, TableSource> tableSources;
    private final Map<String, TableSink<?>> tableSinks;
    private final Map<String, Catalog> catalogs;
    private final Map<String, UserDefinedFunction> functions;
    private final Configuration flinkConfig;
    private final CommandLine commandLine;
    private final CustomCommandLine<T> activeCommandLine;
    private final RunOptions runOptions;
    private final T clusterId;
    private final ClusterSpecification clusterSpec;

    private final ExecutionContext.EnvironmentInstance environmentInstance;

    /**
     * 默认10分钟间隔checkpoint
     */
    private final long DEFAULT_CHECKPOINT_INTERVAL = 10 * 60_000L;
    private final boolean DEFAULT_ENABLE_CHECKPOINT = true;
    private final String DEFAULT_JOB_RUN_NAME = "jobName";

    private final JobRunConfig DEFAULT_JOB_RUN_CONFIG = JobRunConfig.builder()
            .jobName(DEFAULT_JOB_RUN_NAME)
            .checkpointInterval(DEFAULT_CHECKPOINT_INTERVAL)
            .build();

    public ExecutionContext(Environment defaultEnvironment, SessionContext sessionContext, List<File> dependencies,
                            Configuration flinkConfig, Options commandLineOptions, List<CustomCommandLine<?>> availableCommandLines, JobRunConfig jobRunConfig) throws MalformedURLException {
        this.sessionContext = sessionContext.copy(); // create internal copy because session context is mutable
        this.mergedEnv = Environment.merge(defaultEnvironment, sessionContext.getEnvironment());
        this.dependencies = dependencies;
        this.flinkConfig = flinkConfig;

        List<URL> dependencyUrls = JarUtils.toURLs(dependencies);

        // create class loader 并设定当前线程classloader
        classLoader = FlinkUserCodeClassLoaders.parentFirst(
                dependencyUrls.toArray(new URL[dependencies.size()]),
                this.getClass().getClassLoader());
        Thread.currentThread().setContextClassLoader(classLoader);

        // create catalogs
        catalogs = new LinkedHashMap<>();
        mergedEnv.getCatalogs().forEach((name, entry) ->
                catalogs.put(name, createCatalog(name, entry.asMap(), classLoader))
        );

        // create table sources & sinks.
        tableSources = new LinkedHashMap<>();
        tableSinks = new LinkedHashMap<>();
        mergedEnv.getTables().forEach((name, entry) -> {
            if (entry instanceof SourceTableEntry || entry instanceof SourceSinkTableEntry) {
                tableSources.put(name, createTableSource(mergedEnv.getExecution(), entry.asMap(), classLoader));
            }
            if (entry instanceof SinkTableEntry || entry instanceof SourceSinkTableEntry) {
                tableSinks.put(name, createTableSink(mergedEnv.getExecution(), entry.asMap(), classLoader));
            }
        });

        // create user-defined functions
        functions = new LinkedHashMap<>();
        mergedEnv.getFunctions().forEach((name, entry) -> {
            final UserDefinedFunction function = FunctionService.createFunction(entry.getDescriptor(), classLoader, false);
            functions.put(name, function);
        });

        // convert deployment options into command line options that describe a cluster
        commandLine = createCommandLine(mergedEnv.getDeployment(), commandLineOptions);
        activeCommandLine = findActiveCommandLine(availableCommandLines, commandLine);
        runOptions = createRunOptions(commandLine);
        clusterId = activeCommandLine.getClusterId(commandLine);
        clusterSpec = createClusterSpecification(activeCommandLine, commandLine);

        // always share environment instance
        if ( null == jobRunConfig ) {
            environmentInstance = new ExecutionContext.EnvironmentInstance(DEFAULT_ENABLE_CHECKPOINT, DEFAULT_JOB_RUN_CONFIG);
        } else {
            environmentInstance = new ExecutionContext.EnvironmentInstance(DEFAULT_ENABLE_CHECKPOINT, jobRunConfig);
        }
    }

    public SessionContext getSessionContext() {
        return sessionContext;
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    public Environment getMergedEnvironment() {
        return mergedEnv;
    }

    public ClusterSpecification getClusterSpec() {
        return clusterSpec;
    }

    public T getClusterId() {
        return clusterId;
    }

    public ClusterDescriptor<T> createClusterDescriptor() throws Exception {
        return activeCommandLine.createClusterDescriptor(commandLine);
    }

    public EnvironmentInstance getEnvironmentInstance() {
        return environmentInstance;
    }

    public ExecutionContext.EnvironmentInstance createEnvironmentInstance() {
        if (environmentInstance != null) {
            return environmentInstance;
        }
        try {
            return new ExecutionContext.EnvironmentInstance(DEFAULT_ENABLE_CHECKPOINT, DEFAULT_JOB_RUN_CONFIG);
        } catch (Throwable t) {
            // catch everything such that a wrong environment does not affect invocations
            throw new SqlExecutionException("Could not create environment instance.", t);
        }
    }

    public Map<String, TableSource> getTableSources() {
        return tableSources;
    }

    public Map<String, TableSink<?>> getTableSinks() {
        return tableSinks;
    }

    /**
     * Executes the given supplier using the execution context's classloader as thread classloader.
     */
    public <R> R wrapClassLoader(Supplier<R> supplier) {
        try (TemporaryClassLoaderContext tmpCl = new TemporaryClassLoaderContext(classLoader)){
            return supplier.get();
        }
    }

    // --------------------------------------------------------------------------------------------

    private static CommandLine createCommandLine(DeploymentEntry deployment, Options commandLineOptions) {
        try {
            return deployment.getCommandLine(commandLineOptions);
        } catch (Exception e) {
            throw new SqlExecutionException("Invalid deployment options.", e);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> CustomCommandLine<T> findActiveCommandLine(List<CustomCommandLine<?>> availableCommandLines, CommandLine commandLine) {
        for (CustomCommandLine<?> cli : availableCommandLines) {
            if (cli.isActive(commandLine)) {
                return (CustomCommandLine<T>) cli;
            }
        }
        throw new SqlExecutionException("Could not find a matching deployment.");
    }

    private static RunOptions createRunOptions(CommandLine commandLine) {
        try {
            return new RunOptions(commandLine);
        } catch (CliArgsException e) {
            throw new SqlExecutionException("Invalid deployment run options.", e);
        }
    }

    private static ClusterSpecification createClusterSpecification(CustomCommandLine<?> activeCommandLine, CommandLine commandLine) {
        try {
            return activeCommandLine.getClusterSpecification(commandLine);
        } catch (FlinkException e) {
            throw new SqlExecutionException("Could not create cluster specification for the given deployment.", e);
        }
    }

    private Catalog createCatalog(String name, Map<String, String> catalogProperties, ClassLoader classLoader) {
        final CatalogFactory factory =
                TableFactoryService.find(CatalogFactory.class, catalogProperties, classLoader);
        return factory.createCatalog(name, catalogProperties);
    }

    private static TableSource createTableSource(ExecutionEntry execution, Map<String, String> sourceProperties, ClassLoader classLoader) {
        if (execution.isStreamingPlanner()) {
            final StreamTableSourceFactory<?> factory = (StreamTableSourceFactory<?>)
                    TableFactoryService.find(StreamTableSourceFactory.class, sourceProperties, classLoader);
            return factory.createStreamTableSource(sourceProperties);
        } else if (execution.isBatchPlanner()) {
            final BatchTableSourceFactory<?> factory = (BatchTableSourceFactory<?>)
                    TableFactoryService.find(BatchTableSourceFactory.class, sourceProperties, classLoader);
            return factory.createBatchTableSource(sourceProperties);
        }
        throw new SqlExecutionException("Unsupported execution type for sources.");
    }

    private static TableSink<?> createTableSink(ExecutionEntry execution, Map<String, String> sinkProperties, ClassLoader classLoader) {
        if (execution.isStreamingPlanner()) {
            final StreamTableSinkFactory<?> factory = (StreamTableSinkFactory<?>)
                    TableFactoryService.find(StreamTableSinkFactory.class, sinkProperties, classLoader);
            return factory.createStreamTableSink(sinkProperties);
        } else if (execution.isBatchPlanner()) {
            final BatchTableSinkFactory<?> factory = (BatchTableSinkFactory<?>)
                    TableFactoryService.find(BatchTableSinkFactory.class, sinkProperties, classLoader);
            return factory.createBatchTableSink(sinkProperties);
        }
        throw new SqlExecutionException("Unsupported execution type for sinks.");
    }

    private static TableEnvironment createStreamTableEnvironment(
            StreamExecutionEnvironment env,
            EnvironmentSettings settings,
            org.apache.flink.table.delegation.Executor executor) {

        final TableConfig config = TableConfig.getDefault();

        final CatalogManager catalogManager = new CatalogManager(
                settings.getBuiltInCatalogName(),
                new GenericInMemoryCatalog(settings.getBuiltInCatalogName(), settings.getBuiltInDatabaseName()));

        final FunctionCatalog functionCatalog = new FunctionCatalog(catalogManager);

        final Map<String, String> plannerProperties = settings.toPlannerProperties();
        final Planner planner = ComponentFactoryService.find(PlannerFactory.class, plannerProperties)
                .create(plannerProperties, executor, config, functionCatalog, catalogManager);

        return new StreamTableEnvironmentImpl(
                catalogManager,
                functionCatalog,
                config,
                env,
                planner,
                executor,
                settings.isStreamingMode()
        );
    }

    private static org.apache.flink.table.delegation.Executor lookupExecutor(
            Map<String, String> executorProperties,
            StreamExecutionEnvironment executionEnvironment) {
        try {
            ExecutorFactory executorFactory = ComponentFactoryService.find(ExecutorFactory.class, executorProperties);
            Method createMethod = executorFactory.getClass()
                    .getMethod("create", Map.class, StreamExecutionEnvironment.class);

            return (Executor) createMethod.invoke(
                    executorFactory,
                    executorProperties,
                    executionEnvironment);
        } catch (Exception e) {
            throw new TableException(
                    "Could not instantiate the executor. Make sure a planner module is on the classpath",
                    e);
        }
    }

    // --------------------------------------------------------------------------------------------

    /**
     * {@link ExecutionEnvironment} and {@link StreamExecutionEnvironment} cannot be reused
     * across multiple queries because they are stateful. This class abstracts execution
     * environments and table environments.
     */
    public class EnvironmentInstance {

        private final QueryConfig queryConfig;
        private final ExecutionEnvironment execEnv;
        private final StreamExecutionEnvironment streamExecEnv;
        private final Executor executor;
        private final TableEnvironment tableEnv;

        private EnvironmentInstance(Boolean enableCheckpoint, JobRunConfig jobRunConfig) {
            // create settings
            final EnvironmentSettings settings = mergedEnv.getExecution().getEnvironmentSettings();

            // create environments
            if (mergedEnv.getExecution().isStreamingPlanner()) {
                streamExecEnv = createStreamExecutionEnvironment(enableCheckpoint, jobRunConfig.getCheckpointInterval(), jobRunConfig.getDefaultParallelism());
                execEnv = null;

                final Map<String, String> executorProperties = settings.toExecutorProperties();
                executor = lookupExecutor(executorProperties, streamExecEnv);
                tableEnv = createStreamTableEnvironment(streamExecEnv, settings, executor);
            } else if (mergedEnv.getExecution().isBatchPlanner()) {
                streamExecEnv = null;
                execEnv = createExecutionEnvironment();
                executor = null;
                tableEnv = BatchTableEnvironment.create(execEnv);
            } else {
                throw new SqlExecutionException("Unsupported execution type specified.");
            }

            // register catalogs
            catalogs.forEach(tableEnv::registerCatalog);

            // create query config
            queryConfig = createQueryConfig();

            // register table sources
            tableSources.forEach(tableEnv::registerTableSource);

            // register table sinks
            tableSinks.forEach(tableEnv::registerTableSink);

            // register user-defined functions
            registerFunctions();

            // register views and temporal tables in specified order
            mergedEnv.getTables().forEach((name, entry) -> {
                // if registering a view fails at this point,
                // it means that it accesses tables that are not available anymore
                if (entry instanceof ViewEntry) {
                    final ViewEntry viewEntry = (ViewEntry) entry;
                    registerView(viewEntry);
                } else if (entry instanceof TemporalTableEntry) {
                    final TemporalTableEntry temporalTableEntry = (TemporalTableEntry) entry;
                    registerTemporalTable(temporalTableEntry);
                }
            });

            // can not config source parallelism
            int sourceParallelism = jobRunConfig.getSourceParallelism() > 0 ? jobRunConfig.getSourceParallelism() : jobRunConfig.getDefaultParallelism();

            if (sessionContext.getCurrentCatalog().isPresent()) {
                tableEnv.useCatalog(sessionContext.getCurrentCatalog().get());
            } else if (mergedEnv.getExecution().getCurrentCatalog().isPresent()) {
                tableEnv.useCatalog(mergedEnv.getExecution().getCurrentCatalog().get());
            }
        }

        public QueryConfig getQueryConfig() {
            return queryConfig;
        }

        public StreamExecutionEnvironment getStreamExecutionEnvironment() {
            return streamExecEnv;
        }

        public TableEnvironment getTableEnvironment() {
            return tableEnv;
        }

        public ExecutionConfig getExecutionConfig() {
            return streamExecEnv.getConfig();
        }

        public JobGraph createJobGraph(String name) {

            final FlinkPlan plan = createPlan(name, flinkConfig);

            List<URL> dependenciesURLs;
            try {
                dependenciesURLs = JarUtils.toURLs(dependencies);
            } catch (MalformedURLException e) {
                throw new UncheckedIOException("convert files to urls error: " + dependencies, e);
            }

            JobGraph jobGraph = ClusterClient.getJobGraph(
                    flinkConfig,
                    plan,
                    dependenciesURLs,
                    runOptions.getClasspaths(),
                    runOptions.getSavepointRestoreSettings());

            return jobGraph;
        }

        private FlinkPlan createPlan(String name, Configuration flinkConfig) {
            if (streamExecEnv != null) {
                // special case for Blink planner to apply batch optimizations
                // note: it also modifies the ExecutionConfig!
                if (executor instanceof ExecutorBase) {
                    return ((ExecutorBase) executor).generateStreamGraph(name);
                }
                return streamExecEnv.getStreamGraph(name);
            } else {
                final int parallelism = execEnv.getParallelism();
                final Plan unoptimizedPlan = execEnv.createProgramPlan();
                unoptimizedPlan.setJobName(name);
                final Optimizer compiler = new Optimizer(new DataStatistics(), new DefaultCostEstimator(), flinkConfig);
                return ClusterClient.getOptimizedPlan(compiler, unoptimizedPlan, parallelism);
            }
        }

        private ExecutionEnvironment createExecutionEnvironment() {
            final ExecutionEnvironment execEnv = ExecutionEnvironment.getExecutionEnvironment();
            execEnv.setRestartStrategy(mergedEnv.getExecution().getRestartStrategy());
            execEnv.setParallelism(mergedEnv.getExecution().getParallelism());
            return execEnv;
        }

        private StreamExecutionEnvironment createStreamExecutionEnvironment(Boolean enableCheckpoint, Long interval, Integer defaultParallelism) {
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setRestartStrategy(mergedEnv.getExecution().getRestartStrategy());
            env.setParallelism(mergedEnv.getExecution().getParallelism());
            env.setMaxParallelism(mergedEnv.getExecution().getMaxParallelism());
            env.setStreamTimeCharacteristic(mergedEnv.getExecution().getTimeCharacteristic());
            if (env.getStreamTimeCharacteristic() == TimeCharacteristic.EventTime) {
                env.getConfig().setAutoWatermarkInterval(mergedEnv.getExecution().getPeriodicWatermarksInterval());
            }

            if (Objects.nonNull(enableCheckpoint) && enableCheckpoint){
                if (Objects.nonNull(interval) && interval > 0){
                    env.enableCheckpointing(interval);
                } else {
                    env.enableCheckpointing(DEFAULT_CHECKPOINT_INTERVAL);
                }
            }

            if (Objects.nonNull(defaultParallelism)){
                env.setParallelism(defaultParallelism);
            }

            return env;
        }

        private QueryConfig createQueryConfig() {
            if (mergedEnv.getExecution().isStreamingPlanner()) {
                final StreamQueryConfig config = new StreamQueryConfig();
                final long minRetention = mergedEnv.getExecution().getMinStateRetention();
                final long maxRetention = mergedEnv.getExecution().getMaxStateRetention();
                config.withIdleStateRetentionTime(Time.milliseconds(minRetention), Time.milliseconds(maxRetention));
                return config;
            } else {
                return new BatchQueryConfig();
            }
        }

        private void registerFunctions() {
            if (tableEnv instanceof StreamTableEnvironment) {
                StreamTableEnvironment streamTableEnvironment = (StreamTableEnvironment) tableEnv;
                functions.forEach((k, v) -> {
                    if (v instanceof ScalarFunction) {
                        streamTableEnvironment.registerFunction(k, (ScalarFunction) v);
                    } else if (v instanceof AggregateFunction) {
                        streamTableEnvironment.registerFunction(k, (AggregateFunction<?, ?>) v);
                    } else if (v instanceof TableFunction) {
                        streamTableEnvironment.registerFunction(k, (TableFunction<?>) v);
                    } else {
                        throw new SqlExecutionException("Unsupported function type: " + v.getClass().getName());
                    }
                });
            } else {
                BatchTableEnvironment batchTableEnvironment = (BatchTableEnvironment) tableEnv;
                functions.forEach((k, v) -> {
                    if (v instanceof ScalarFunction) {
                        batchTableEnvironment.registerFunction(k, (ScalarFunction) v);
                    } else if (v instanceof AggregateFunction) {
                        batchTableEnvironment.registerFunction(k, (AggregateFunction<?, ?>) v);
                    } else if (v instanceof TableFunction) {
                        batchTableEnvironment.registerFunction(k, (TableFunction<?>) v);
                    } else {
                        throw new SqlExecutionException("Unsupported function type: " + v.getClass().getName());
                    }
                });
            }
        }

        private void registerView(ViewEntry viewEntry) {
            try {
                tableEnv.registerTable(viewEntry.getName(), tableEnv.sqlQuery(viewEntry.getQuery()));
            } catch (Exception e) {
                throw new SqlExecutionException(
                        "Invalid view '" + viewEntry.getName() + "' with query:\n" + viewEntry.getQuery()
                                + "\nCause: " + e.getMessage());
            }
        }

        private void registerTemporalTable(TemporalTableEntry temporalTableEntry) {
            try {
                final Table table = tableEnv.scan(temporalTableEntry.getHistoryTable());
                final TableFunction<?> function = table.createTemporalTableFunction(
                        temporalTableEntry.getTimeAttribute(),
                        String.join(",", temporalTableEntry.getPrimaryKeyFields()));
                if (tableEnv instanceof StreamTableEnvironment) {
                    StreamTableEnvironment streamTableEnvironment = (StreamTableEnvironment) tableEnv;
                    streamTableEnvironment.registerFunction(temporalTableEntry.getName(), function);
                } else {
                    BatchTableEnvironment batchTableEnvironment = (BatchTableEnvironment) tableEnv;
                    batchTableEnvironment.registerFunction(temporalTableEntry.getName(), function);
                }
            } catch (Exception e) {
                throw new SqlExecutionException(
                        "Invalid temporal table '" + temporalTableEntry.getName() + "' over table '" +
                                temporalTableEntry.getHistoryTable() + ".\nCause: " + e.getMessage());
            }
        }
    }
}
