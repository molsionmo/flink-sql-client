package com.github.mxb.flink.sql.cluster.model.run.overview;

/**
 * <p>JobRunStatusEnum</p>
 *
 * @author moxianbin
 * @since 2020/4/9 16:40
 */
public enum JobRunStatusEnum {

    /** Job is do not in the cluster */
    NONE(TerminalState.GLOBALLY),

    /** Job is newly created, no task has started to run. */
    CREATED(TerminalState.NON_TERMINAL),

    /** Some tasks are scheduled or running, some may be pending, some may be finished. */
    RUNNING(TerminalState.NON_TERMINAL),

    /** The job has failed and is currently waiting for the cleanup to complete */
    FAILING(TerminalState.NON_TERMINAL),

    /** The job has failed with a non-recoverable task failure */
    FAILED(TerminalState.GLOBALLY),

    /** Job is being cancelled */
    CANCELLING(TerminalState.NON_TERMINAL),

    /** Job has been cancelled */
    CANCELED(TerminalState.GLOBALLY),

    /** All of the job's tasks have successfully finished. */
    FINISHED(TerminalState.GLOBALLY),

    /** The job is currently undergoing a reset and total restart */
    RESTARTING(TerminalState.NON_TERMINAL),

    /** The job has been suspended and is currently waiting for the cleanup to complete */
    SUSPENDING(TerminalState.NON_TERMINAL),

    /**
     * The job has been suspended which means that it has been stopped but not been removed from a potential HA job store.
     * 局部终态
     */
    SUSPENDED(TerminalState.LOCALLY),

    /** The job is currently reconciling and waits for task execution report to recover state. */
    RECONCILING(TerminalState.NON_TERMINAL);

    // --------------------------------------------------------------------------------------------

    private enum TerminalState {

        /**
         * 非局部全局终态
         */
        NON_TERMINAL,

        /**
         * 局部终态
         */
        LOCALLY,

        /**
         * 全局终态
         */
        GLOBALLY
    }

    private final TerminalState terminalState;

    JobRunStatusEnum(TerminalState terminalState) {
        this.terminalState = terminalState;
    }

    /**
     * Checks whether this state is <i>globally terminal</i>. A globally terminal job
     * is complete and cannot fail any more and will not be restarted or recovered by another
     * standby master node.
     *
     * <p>When a globally terminal state has been reached, all recovery data for the job is
     * dropped from the high-availability services.
     *
     * @return True, if this job status is globally terminal, false otherwise.
     */
    public boolean isGloballyTerminalState() {
        return terminalState == TerminalState.GLOBALLY;
    }
}
