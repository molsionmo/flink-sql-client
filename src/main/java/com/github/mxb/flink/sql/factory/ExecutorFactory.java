package com.github.mxb.flink.sql.factory;


import com.github.mxb.flink.sql.executor.Executor;
import com.github.mxb.flink.sql.executor.LocalExecutor;
import com.github.mxb.flink.sql.minicluster.MiniClusterResource;
import com.github.mxb.flink.sql.util.ValidateUtil;
import org.apache.flink.table.api.ValidationException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ExecutorFactory
 *
 * @author moxianbin
 * @since 2020/4/8 18:21
 */
public class ExecutorFactory {

    public static final String PROVIDER_KEY = "provider";

    public static LocalExecutor createExecutor(MiniClusterResource.MiniClusterResourceConfiguration miniClusterConfig) throws Exception {
        Map<String, String> properties = new HashMap<>(miniClusterConfig.toProperties());
        properties.put(PROVIDER_KEY, LocalExecutor.class.getName());

        return (LocalExecutor) createExecutor(properties);
    }

    public static Executor createExecutor(Map<String, String> properties) throws Exception {
        String provider = properties.get(PROVIDER_KEY);

        Class<?> providerClass = Thread.currentThread().getContextClassLoader().loadClass(provider);
        if (!Executor.class.isAssignableFrom(providerClass)) {
            throw new IllegalArgumentException("provider class :" + provider + " have not assign Executor.class");
        }

        Executor template = Executor.class.cast(providerClass.newInstance());
        //validate properties
        validateProperties(template,properties);

        return template.newInstance(properties);
    }

    private static void validateProperties(Executor executor, Map<String, String> properties) {
        List<String> requiredProperties = executor.requiredProperties();
        requiredProperties.forEach(requiredProperty->{
            ValidateUtil.validateOptional(properties, requiredProperty, false);
        });
        List<String> supportedProperties = executor.supportedProperties();
//        properties.forEach((key,value)->{
//            if (!supportedProperties.contains(key)){
//                throw new ValidationException("Not support property '" + key + "'.");
//            }
//        });
    }

}
