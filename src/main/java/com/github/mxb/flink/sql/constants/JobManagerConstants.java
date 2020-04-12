package com.github.mxb.flink.sql.constants;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @description     define executor properties
 * @auther          moxianbin
 * @create          2020-04-11 20:46:37
 */
public class JobManagerConstants {


    public static final String JOB_MANAGE_ADDRESS_KEY = "jobManageAddress";
    public static final String JOB_MANAGE_REST_PORT_KEY = "jobManageRestPort";

    private static final String[] optionalJobRunConfigKey = new String[]{
    };

    private static final String[] requiredJobRunConfigKey = new String[]{
            JOB_MANAGE_ADDRESS_KEY, JOB_MANAGE_REST_PORT_KEY
    };

    public static final Set<String> OPTIONAL_JOB_RUN_CONFIG_KEY = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(optionalJobRunConfigKey)));

    public static final Set<String> REQUIRED_JOB_RUN_CONFIG_KEY = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(requiredJobRunConfigKey)));

}
