package com.github.mxb.flink.sql.local.command;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author moxianbin
 * @since 2020/4/13 14:27
 */
public class LocalExecutorConstants {

    public static final String LOCAL_TM_NUM_KEY = "localTmNum";
    public static final String LOCAL_NUM_SLOTS_PER_TM_KEY = "localNumSlotsPerTm";
    public static final String PROVIDER_KEY = "provider";

    private static final String[] optionalJobRunConfigKey = new String[]{
    };

    private static final String[] requiredJobRunConfigKey = new String[]{
            PROVIDER_KEY, LOCAL_TM_NUM_KEY, LOCAL_NUM_SLOTS_PER_TM_KEY
    };

    public static final Set<String> OPTIONAL_JOB_RUN_CONFIG_KEY = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(optionalJobRunConfigKey)));

    public static final Set<String> REQUIRED_JOB_RUN_CONFIG_KEY = Collections.unmodifiableSet((new HashSet<>(Arrays.asList(requiredJobRunConfigKey))));
}
