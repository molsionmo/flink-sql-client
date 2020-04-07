package com.github.mxb.flink.sql.model.run;

import com.github.mxb.flink.sql.constants.JobManagerConstants;
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

        properties.put(JobManagerConstants.JOB_MANAGE_ADDRESS_KEY, this.jobManageAddress);
        properties.put(JobManagerConstants.JOB_MANAGE_REST_PORT_KEY, String.valueOf(this.jobManageRestPort));

        return properties;
    }
}
