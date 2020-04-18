package com.github.mxb.flink.sql.factory;

import com.github.mxb.flink.sql.cluster.ClusterDescriptor;
import com.github.mxb.flink.sql.cluster.config.YarnClusterConfig;
import com.github.mxb.flink.sql.cluster.descriptor.StandaloneClusterDescriptor;
import com.github.mxb.flink.sql.cluster.descriptor.YarnClusterDescriptor;
import com.github.mxb.flink.sql.cluster.resource.AuthType;
import com.github.mxb.flink.sql.cluster.resource.FlinkResourceInfo;
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
            FlinkResourceInfo flinkResourceInfo) throws IOException {
        return createClusterDescriptor(flinkResourceInfo, new Configuration());
    }

    public static ClusterDescriptor createClusterDescriptor(
            FlinkResourceInfo flinkResourceInfo,
            Configuration flinkConfiguration) throws IOException {
        if (!flinkResourceInfo.getResourceType().isSupported()) {
            throw new UnsupportedOperationException("暂不支持的资源类型:" + flinkResourceInfo.getResourceType().name());
        }

        flinkResourceInfo.validate();

        ClusterDescriptor clusterDescriptor = null;
        if (flinkResourceInfo.getResourceType().equals(ResourceType.YARN)) {
            clusterDescriptor = createYarnClusterDescriptor(flinkResourceInfo, flinkConfiguration);
        } else if (flinkResourceInfo.getResourceType().equals(ResourceType.STANDALONE)) {
            clusterDescriptor = createStandAloneClusterDescriptor(flinkResourceInfo, flinkConfiguration);
        } else {
            throw new UnsupportedOperationException("暂不支持的资源类型:" + flinkResourceInfo.getResourceType().name());
        }

        return clusterDescriptor;
    }

    private static StandaloneClusterDescriptor createStandAloneClusterDescriptor(
            FlinkResourceInfo flinkResourceInfo,
            Configuration flinkConfiguration) throws IOException {
        return new StandaloneClusterDescriptor();
    }

    private static YarnClusterDescriptor createYarnClusterDescriptor(
            FlinkResourceInfo flinkResourceInfo,
            Configuration flinkConfiguration) throws IOException {
        if (flinkResourceInfo.getAuthType() != AuthType.KERBEROS && flinkResourceInfo.getAuthType() != AuthType.NONE) {
            throw new UnsupportedOperationException("暂不支持KERBEROS 与 NONE 以外的认证类型");
        }

        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        String tmpYarnConfPath = copyYarnConfToFile(flinkResourceInfo.getYarnRmAddress(), flinkResourceInfo.getResourceId());
        yarnConfiguration.addResource(new Path(tmpYarnConfPath));

        if (flinkResourceInfo.getAuthType() == AuthType.KERBEROS) {
            // init system property
            System.setProperty(YarnClusterConfig.KRB5_CONF, StringUtils.isBlank(flinkResourceInfo.getKrb5ConfFilePath()) ? DEFAULT_KRB_5_CONF : flinkResourceInfo.getKrb5ConfFilePath());
            System.setProperty(YarnClusterConfig.KRB5_REALM, flinkResourceInfo.getKrb5Realm());
            System.setProperty(YarnClusterConfig.KRB5_KDC, flinkResourceInfo.getKrb5Kdc());
            // yarn configure and kerb5 authentication
            UserGroupInformation.setConfiguration(yarnConfiguration);
            UserGroupInformation.loginUserFromKeytab(flinkResourceInfo.getKeytabPrinciple(), flinkResourceInfo.getKeytabPath());
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
