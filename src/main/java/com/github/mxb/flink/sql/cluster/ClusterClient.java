package com.github.mxb.flink.sql.cluster;

import com.github.mxb.flink.sql.exception.FlinkClientTimeoutException;
import com.github.mxb.flink.sql.model.monitor.JobMonitorOverview;
import com.github.mxb.flink.sql.model.monitor.JobRunStatusEnum;
import com.github.mxb.flink.sql.model.run.JobConfig;
import com.github.mxb.flink.sql.model.run.JobRunConfig;
import com.github.mxb.flink.sql.model.run.ProgramResultDescriptor;
import org.apache.flink.sql.parser.error.SqlParseException;
import org.apache.flink.table.client.gateway.ProgramTargetDescriptor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.util.FlinkException;

import java.io.File;
import java.util.List;
import java.util.Map;

public interface ClusterClient<T> {

    /**
     * cancel job
     *
     * @param jobId        jobId
     * @param savepointDir savepoint dir
     * @return savepointPath savepoint Path
     * @throws FlinkException any flink error
     */
    String cancelJob(String jobId, String savepointDir) throws FlinkException, FlinkClientTimeoutException;

    /**
     * stop job
     * @param jobId
     * @param savepointDir
     * @return
     * @throws FlinkException
     * @throws FlinkClientTimeoutException
     */
    String stopJob(String jobId, String savepointDir) throws FlinkException, FlinkClientTimeoutException;

    /**
     * triggerSavepoint
     *
     * @param jobId        jobId
     * @param savepointDir savepoint dir
     * @return savepointPath savepoint Path
     * @throws FlinkException any flink error
     */
    String triggerSavepoint(String jobId, String savepointDir) throws FlinkException, FlinkClientTimeoutException;

    /**
     * Submits a Flink SQL update sql such as INSERT INTO.
     *
     * @param jobRunConfig     sql job run config
     * @param dependencyJarDir dependency jar dir,dynamic load dependency connector
     * @param sql              full SQL with DDL and update statement;currently only INSERT INTO is supported
     * @return information about the target of the submitted Flink job
     * @throws FlinkException        any flink error
     * @throws SqlExecutionException sql error
     * @throws SqlParseException     sql format error
     */
    ProgramTargetDescriptor executeSqlJob(JobRunConfig jobRunConfig, String dependencyJarDir, String sql) throws FlinkException, SqlExecutionException, SqlParseException, FlinkClientTimeoutException;

    /**
     *
     * @param jobConfig
     * @param dependencyJars
     * @param sql
     * @return
     * @throws FlinkException
     * @throws SqlExecutionException
     * @throws SqlParseException
     */
    ProgramTargetDescriptor executeSqlJob(JobConfig jobConfig, List<File> dependencyJars, String sql) throws FlinkException, SqlExecutionException, SqlParseException, FlinkClientTimeoutException;

    /**
     *
     * @param jobRunConfig
     * @param dependencyJars
     * @param sql
     * @return
     * @throws FlinkException
     * @throws SqlExecutionException
     * @throws SqlParseException
     */
    ProgramTargetDescriptor executeSqlJob(JobRunConfig jobRunConfig, List<File> dependencyJars, String sql) throws FlinkException, SqlExecutionException, SqlParseException, FlinkClientTimeoutException;

    /**
     * Submits a Flink SELECT or INSERT SQL job internally, the job is not detached that can get the result in the end
     *
     * @param jobRunConfig     sql job run config
     * @param dependencyJarDir dependency jar dir,dynamic load dependency connector
     * @param sql              full SQL with DDL and update statement;currently SELECT and INSERT is supported, if INSERT then change the sink to local result restore
     * @return information about the target of the submitted Flink job
     * @throws FlinkException        any flink error
     * @throws SqlExecutionException sql error
     * @throws SqlParseException     sql format error
     */
    ProgramResultDescriptor executeQueryInternal(JobRunConfig jobRunConfig, String dependencyJarDir, String sql) throws FlinkException, SqlExecutionException, SqlParseException;

    /**
     * retrieveResult by jobId
     *
     * @param jobId jobId
     * @return ProgramResultDescriptor
     * @throws FlinkException any flink error
     */
    ProgramResultDescriptor retrieveResult(String jobId) throws FlinkException;

    /**
     * get jobs runStatus
     *
     * @param jobIds jobId list
     * @return map of jobId and runStatus
     * @throws FlinkException any flink error
     */
    @Deprecated
    Map<String, String> getJobsRunStatus(List<String> jobIds) throws FlinkException, FlinkClientTimeoutException;

    /**
     * get jobs runStatus
     *
     * @param jobIds
     * @return map of jobId and runStatus
     * @throws FlinkException
     * @throws FlinkClientTimeoutException
     */
    Map<String, JobRunStatusEnum> getJobStatus(List<String> jobIds) throws FlinkException, FlinkClientTimeoutException;

    /**
     * get jobs overview
     *
     * @param jobIds jobId list
     * @return map of jobId and jobMonitorOverview
     * @throws FlinkException any flink error
     */
    Map<String, JobMonitorOverview> getJobsOverview(List<String> jobIds) throws FlinkException, FlinkClientTimeoutException;

    /**
     * getJobsOverviewUrl
     *
     * @param jobIds jobId list
     * @return map of jobId and jobOverviewUrl
     */
    Map<String, String> getJobsOverviewUrl(List<String> jobIds);

}
