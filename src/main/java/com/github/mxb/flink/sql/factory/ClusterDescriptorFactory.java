package com.github.mxb.flink.sql.factory;

import com.github.mxb.flink.sql.cluster.ClusterDescriptor;
import com.github.mxb.flink.sql.cluster.config.YarnClusterConfig;
import com.github.mxb.flink.sql.cluster.descriptor.StandaloneClusterDescriptor;
import com.github.mxb.flink.sql.cluster.descriptor.YarnClusterDescriptor;
import com.github.mxb.flink.sql.cluster.resource.AuthType;
import com.github.mxb.flink.sql.cluster.resource.ResourceInfo;
import com.github.mxb.flink.sql.cluster.resource.ResourceType;
import com.github.mxb.flink.sql.util.OkHttpUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.UUID;

@Slf4j
public class ClusterDescriptorFactory {

    private static final String DEFAULT_KRB_5_CONF = "/etc/krb5.conf";

    public static ClusterDescriptor createClusterDescriptor(
            ResourceInfo resourceInfo) throws IOException {
        return createClusterDescriptor(resourceInfo, new Configuration());
    }

    public static ClusterDescriptor createClusterDescriptor(
            ResourceInfo resourceInfo,
            Configuration flinkConfiguration) throws IOException {
        if (!resourceInfo.getResourceType().isSupported()) {
            throw new UnsupportedOperationException("暂不支持的资源类型:" + resourceInfo.getResourceType().name());
        }

        resourceInfo.validate();

        ClusterDescriptor clusterDescriptor = null;
        if (resourceInfo.getResourceType().equals(ResourceType.YARN)) {
            clusterDescriptor = createYarnClusterDescriptor(resourceInfo, flinkConfiguration);
        } else if (resourceInfo.getResourceType().equals(ResourceType.STANDALONE)) {
            clusterDescriptor = createStandAloneClusterDescriptor(resourceInfo, flinkConfiguration);
        } else {
            throw new UnsupportedOperationException("暂不支持的资源类型:" + resourceInfo.getResourceType().name());
        }

        return clusterDescriptor;
    }

    private static StandaloneClusterDescriptor createStandAloneClusterDescriptor(
            ResourceInfo resourceInfo,
            Configuration flinkConfiguration) throws IOException {
        return new StandaloneClusterDescriptor();
    }

    private static YarnClusterDescriptor createYarnClusterDescriptor(
            ResourceInfo resourceInfo,
            Configuration flinkConfiguration) throws IOException {
        if (resourceInfo.getAuthType() != AuthType.KERBEROS && resourceInfo.getAuthType() != AuthType.NONE) {
            throw new UnsupportedOperationException("暂不支持KERBEROS 与 NONE 以外的认证类型");
        }

        YarnConfiguration yarnConfiguration = new YarnConfiguration();

        if (resourceInfo.getAuthType() == AuthType.KERBEROS) {
            String tmpYarnConfPath = copyYarnConfToFile(resourceInfo.getYarnRmAddress(), resourceInfo.getResourceId());
            yarnConfiguration.addResource(new Path(tmpYarnConfPath));
            // init system property
            System.setProperty(YarnClusterConfig.KRB5_CONF, StringUtils.isBlank(resourceInfo.getKrb5ConfFilePath()) ? DEFAULT_KRB_5_CONF : resourceInfo.getKrb5ConfFilePath());
            System.setProperty(YarnClusterConfig.KRB5_REALM, resourceInfo.getKrb5Realm());
            System.setProperty(YarnClusterConfig.KRB5_KDC, resourceInfo.getKrb5Kdc());
            // yarn configure and kerb5 authentication
            UserGroupInformation.setConfiguration(yarnConfiguration);
            UserGroupInformation.loginUserFromKeytab(resourceInfo.getKeytabPrinciple(), resourceInfo.getKeytabPath());
        }

        // start yarn client
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConfiguration);
        yarnClient.start();

        return new YarnClusterDescriptor(
                flinkConfiguration,
                yarnConfiguration,
                yarnClient
        );
    }

    private static String copyYarnConfToFile(String host, String resourceId) throws IOException {
        String tmpdir = System.getProperty("java.io.tmpdir");
        String tmpFileName = resourceId + "_" + UUID.randomUUID() + "_" + "tmp_yarn_site.xml";
        File tmpFile = new File(tmpdir, tmpFileName);

        String conf = getYarnConf(host);

        try {
            FileUtils.writeStringToFile(tmpFile, conf);
        } catch (IOException e) {
            log.error("yarn conf写入tmp文件失败", e);
        }
        return tmpFile.getAbsolutePath();
    }

    private static String getYarnConf(String host) throws IOException {
        String url = MessageFormat.format("{0}/conf", host);
        return OkHttpUtils.get(url, String.class);
    }
}
