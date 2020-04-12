package com.github.mxb.flink.sql.model.run;

import lombok.Builder;
import lombok.Data;

/**
 * job run config
 *
 * @author moxianbin
 * @since 2020/4/8 16:20
 */
@Data
@Builder
public class JobRunConfig {

    /**
     * job名称
     */
    private String jobName;

    /**
     * checkpoint间隔;单位:ms;默认10min
     */
    private long checkpointInterval;

    /**
     * 用于重启当前任务job的savePoint路径;非重启任务时为空
     */
    private String restoreSavePointPath;

    /**
     * If you dropped an operator, you can allow to skip state that cannot be mapped to the new program
     *
     * if true,restore savepoint will skip state if you dropped an operator
     * if false,restore savepoint will not skip state if you dropped an operator and run with Exception
     */
    private Boolean isAllowNonRestoredState;

    /**
     * defaultParallelism;
     */
    private int defaultParallelism;

    /**
     * sourceParallelism;
     * can not config source parallelism
     */
    private int sourceParallelism;

    /**
     * BATCH OR STREAMING
     */
    private JobRunType jobRunType;

    private Boolean isDetached = true;

    //TODO restart strategy config
}
