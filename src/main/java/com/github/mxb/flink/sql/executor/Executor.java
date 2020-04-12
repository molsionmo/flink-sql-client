package com.github.mxb.flink.sql.executor;

import com.github.mxb.flink.sql.model.monitor.JobMonitorOverview;
import com.github.mxb.flink.sql.model.run.JobConfig;
import com.github.mxb.flink.sql.model.run.JobRunConfig;
import com.github.mxb.flink.sql.model.run.ProgramResultDescriptor;
import org.apache.flink.sql.parser.error.SqlParseException;
import org.apache.flink.table.client.gateway.ProgramTargetDescriptor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.util.FlinkException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * <p>A gateway for communicating with Flink and other external systems.</p>
 *
 * @author moxianbin
 * @since 2020/4/8 15:17
 */
public interface Executor {

    /**
     * Starts the com.ppmon.bigdata.flink.sql.client.executor and ensures that its is ready for commands to be executed.
     *
     * @throws SqlExecutionException
     */
    void start() throws SqlExecutionException;

    /**
     * cancel job
     *
     * @param jobId
     * @param savepointDir
     * @return savepointPath
     */
    String cancelJob(String jobId, String savepointDir) throws FlinkException;

    /**
     * triggerSavepoint
     *
     * @param jobId
     * @param savepointDir
     * @return savepointPath
     */
    String triggerSavepoint(String jobId, String savepointDir) throws FlinkException, ExecutionException, InterruptedException;

    Executor newInstance(Map<String, String> properties) throws Exception;

    /**
     * definition the executor support properties
     *
     * @return
     */
    List<String> supportedProperties();

    /**
     * Specifies properties that this executor must have
     *
     * <p>An empty context means that the factory matches for all requests.
     *
     * @return
     */
    List<String> requiredProperties();

    /**
     * Submits a Flink SQL update sql such as INSERT INTO.
     *
     * @param jobRunConfig     sql job run config
     * @param dependencyJarDir dependency jar dir,dynamic load dependency connector
     * @param sql              full SQL with DDL and update statement;currently only INSERT INTO is supported
     * @return information about the target of the submitted Flink job
     */
    ProgramTargetDescriptor executeSqlJob(JobRunConfig jobRunConfig, String dependencyJarDir, String sql) throws SqlExecutionException, SqlParseException;

    /**
     * Submits a Flink SQL update sql such as INSERT INTO.
     *
     * @param jobConfig        作业配置
     * @param dependencyJarDir dependency jar dir,dynamic load dependency connector
     * @param sql              full SQL with DDL and update statement;currently only INSERT INTO is supported
     * @return information about the target of the submitted Flink job
     * @throws SqlExecutionException
     * @throws SqlParseException
     */
    ProgramTargetDescriptor executeSqlJob(JobConfig jobConfig, String dependencyJarDir, String sql) throws SqlExecutionException, SqlParseException;


    /**
     * Submits a Flink SELECT or INSERT SQL job internally, the job is not detached that can get the result in the end
     *
     * @param jobRunConfig     sql job run config
     * @param dependencyJarDir dependency jar dir,dynamic load dependency connector
     * @param sql              full SQL with DDL and update statement;currently SELECT and INSERT is supported, if INSERT then change the sink to local result restore
     * @return
     * @throws SqlExecutionException
     * @throws SqlParseException
     */
    ProgramResultDescriptor executeQueryInternal(JobRunConfig jobRunConfig, String dependencyJarDir, String sql) throws SqlExecutionException, SqlParseException;

    /**
     * retrieveResult by jobId
     *
     * @param jobId
     * @return ProgramResultDescriptor
     */
    ProgramResultDescriptor retrieveResult(String jobId);

    /**
     * get jobs runStatus
     *
     * @param jobIds
     * @return key:jobId value:runStatus
     * @throws IOException
     */
    Map<String, String> getJobsRunStatus(List<String> jobIds) throws IOException;

    /**
     * get jobs overview
     *
     * @param jobIds
     * @return
     */
    Map<String, JobMonitorOverview> getJobsOverview(List<String> jobIds);
}
