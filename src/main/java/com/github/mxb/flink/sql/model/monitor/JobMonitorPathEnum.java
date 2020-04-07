package com.github.mxb.flink.sql.model.monitor;

import java.util.List;

public enum JobMonitorPathEnum {
    
    JM_CONFIG("/jobmanager/config", List.class),
    JM_JOBS("/jobs", JobsRunStatus.class),
    JOB_OVERVIEW("/jobs/{jobId}", JobMonitorOverview.class),
    ;

    private String path;
    private Class cls;

    public String getPath() {
        return path;
    }

    public Class getCls() {
        return cls;
    }

    JobMonitorPathEnum(String path, Class cls) {
        this.path = path;
        this.cls = cls;
    }
}
