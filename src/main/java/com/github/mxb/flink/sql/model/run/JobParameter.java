package com.github.mxb.flink.sql.model.run;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@Builder
public class JobParameter {

    /**
     * 作业参数
     */
    private Map<String, String> parameters;
}
