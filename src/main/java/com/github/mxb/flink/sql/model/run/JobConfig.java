package com.github.mxb.flink.sql.model.run;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;

import java.util.Map;

@Data
@Builder
@Getter
public class JobConfig {

    /**
     * 作业运行时配置
     */
    @NonNull
    private JobRunConfig jobRunConfig;

    /**
     * 作业运行时参数
     */
    @NonNull
    private Map<String, String> jobParameter;

    public JobConfig(JobRunConfig jobRunConfig, Map<String, String> jobParameter) {
        if (jobRunConfig == null || jobParameter == null) {
            throw new IllegalArgumentException("jobRunConfig或jobParameter不能为空");
        }

        this.jobRunConfig = jobRunConfig;
        this.jobParameter = jobParameter;
    }
}
