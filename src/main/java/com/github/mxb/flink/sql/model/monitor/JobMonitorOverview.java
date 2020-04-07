package com.github.mxb.flink.sql.model.monitor;

import lombok.Data;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * job monitor overview structure
 */
@Data
public class JobMonitorOverview {

    private String jid;

    private String name;

    private boolean isStoppable;

    private String state;

    /**
     * 运行时间
     */
    private long duration;

    @JsonProperty("start_time")
    private long startTime;
    @JsonProperty("end_time")
    private long endTime;
    private long now;

    private Timestamps timestamps;
    @JsonProperty("status-counts")
    private StatusCounts statusCounts;
    private Plan plan;
    private List<Vertices> vertices;

    @Data
    public static class StatusCounts {
        @JsonProperty("CREATED")
        private int created;
        @JsonProperty("FINISHED")
        private int finished;
        @JsonProperty("DEPLOYING")
        private int deploying;
        @JsonProperty("FAILED")
        private int failed;
        @JsonProperty("RECONCILING")
        private int reconciling;
        @JsonProperty("CANCELED")
        private int canceled;
        @JsonProperty("SCHEDULED")
        private int scheduled;
        @JsonProperty("RUNNING")
        private int running;
        @JsonProperty("CANCELING")
        private int canceling;
    }

    @Data
    public static class Timestamps {
        @JsonProperty("FAILED")
        private long failed;
        @JsonProperty("CANCELED")
        private long canceled;
        @JsonProperty("FINISHED")
        private long finished;
        @JsonProperty("RECONCILING")
        private long reconciling;
        @JsonProperty("SUSPENDING")
        private long suspending;
        @JsonProperty("SUSPENDED")
        private long suspended;
        @JsonProperty("CREATED")
        private long created;
        @JsonProperty("RUNNING")
        private long running;
        @JsonProperty("CANCELLING")
        private long cancelling;
        @JsonProperty("FAILING")
        private long failing;
        @JsonProperty("RESTARTING")
        private long restarting;
    }

    @Data
    public static class Plan {
        private String jid;
        private String name;
        private List<Nodes> nodes;
    }

    @Data
    public static class Nodes {
        private String id;
        private int parallelism;
        private String operator;
        @JsonProperty("operator_strategy")
        private String operatorStrategy;
        private String description;
        @JsonProperty("optimizerProperties")
        private Map<String,Object> optimizerProperties;
    }

    @Data
    public static class Vertices{
        private String id;
        private String name;
        private int parallelism;
        private String status;
        @JsonProperty("start-time")
        private long startTime;
        @JsonProperty("end-time")
        private long endTime;
        private long duration;
        private Tasks tasks;
        private Metrics metrics;
    }

    @Data
    public static class Metrics{
        @JsonProperty("read-bytes")
        private long readBytes;
        @JsonProperty("read-bytes-complete")
        private boolean readBytesComplete;
        @JsonProperty("read-bytes-complete")
        private long writeBytes;
        @JsonProperty("write-bytes-complete")
        private boolean writeBytesComplete;
        @JsonProperty("read-records")
        private long readRecords;
        @JsonProperty("read-records-complete")
        private boolean readRecordsComplete;
        @JsonProperty("write-records")
        private long writeRecords;
        @JsonProperty("write-records-complete")
        private boolean writeRecordsComplete;
        @JsonProperty("buffers-in-pool-usage-max")
        private double buffersInPoolUsageMax;
        @JsonProperty("buffers-in-pool-usage-max-complete")
        private boolean buffersInPoolUsageMaxComplete;
        @JsonProperty("buffers-out-pool-usage-max")
        private double buffersOutPoolUsageMax;
        @JsonProperty("buffers-out-pool-usage-max-complete")
        private boolean buffersOutPoolUsageMaxComplete;
        private double tps;
        @JsonProperty("tps-complete")
        private boolean tpsComplete;
        private long delay;
        @JsonProperty("delay-complete")
        private boolean delayComplete;
        private Map<String,String> userScope;
    }

    @Data
    public static class Tasks{
        @JsonProperty("CREATED")
        private int created;
        @JsonProperty("FINISHED")
        private int finished;
        @JsonProperty("DEPLOYING")
        private int deploying;
        @JsonProperty("FAILED")
        private int failed;
        @JsonProperty("RECONCILING")
        private int reconciling;
        @JsonProperty("CANCELED")
        private int canceled;
        @JsonProperty("SCHEDULED")
        private int scheduled;
        @JsonProperty("RUNNING")
        private int running;
        @JsonProperty("CANCELING")
        private int canceling;
    }
}
