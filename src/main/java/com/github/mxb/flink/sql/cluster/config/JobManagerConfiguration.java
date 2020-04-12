package com.github.mxb.flink.sql.cluster.config;

import lombok.Builder;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * <p></p>
 *
 * @author moxianbin
 */
@Data
@Builder
public class JobManagerConfiguration {

    public static final String JOB_MANAGE_ADDRESS_KEY = "jobManageAddress";
    public static final String JOB_MANAGE_REST_PORT_KEY = "jobManageRestPort";

    /**
     * jobManageAddress
     */
    private String jobManageAddress;

    /**
     * jobManageRestPort
     */
    private Integer jobManageRestPort;

    public Map<String, String> toProperties(){
        Map<String, String> properties = new HashMap<>();

        properties.put(JOB_MANAGE_ADDRESS_KEY, this.jobManageAddress);
        properties.put(JOB_MANAGE_ADDRESS_KEY, String.valueOf(this.jobManageRestPort));

        return properties;
    }
}
