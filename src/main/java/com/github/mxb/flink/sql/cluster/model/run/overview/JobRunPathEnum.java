package com.github.mxb.flink.sql.cluster.model.run.overview;

import java.util.List;

public enum JobRunPathEnum {
    
    JM_CONFIG("/jobmanager/config", List.class),
    JM_JOBS("/jobs", JobsRunStatus.class),
    JOB_OVERVIEW("/jobs/{jobId}", JobRunOverview.class),
    ;

    private String path;
    private Class cls;

    public String getPath() {
        return path;
    }

    public Class getCls() {
        return cls;
    }

    JobRunPathEnum(String path, Class cls) {
        this.path = path;
        this.cls = cls;
    }
}
