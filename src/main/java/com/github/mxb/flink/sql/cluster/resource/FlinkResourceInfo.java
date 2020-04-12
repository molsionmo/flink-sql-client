package com.github.mxb.flink.sql.cluster.resource;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

/**
 * @description     Flink Resource Info that can get the resource application
 * @auther          moxianbin
 * @create          2020-04-11 19:12:28
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FlinkResourceInfo {

    private String resourceId;
    private ResourceType resourceType;
    private AuthType authType;

    /**
     * authType为KERBEROS时生效，例：192.168.1.1
     */
    private String krb5Kdc;
    /**
     * authType为KERBEROS时生效，例：192.168.1.1
     */
    private String krb5Realm;
    /**
     * authType为KERBEROS时生效，例：/data/my.keytab
     */
    private String keytabPath;
    /**
     * authType为KERBEROS时生效，例：hdfs@REAL.IO
     */
    private String keytabPrinciple;
    /**
     * authType为KERBEROS时生效，例: /etc/krb5.conf
     */
    private String krb5ConfFilePath;


    /**
     * resourceType为YARN时生效，yarn的resourcemanager的webapp地址，例：http://192.168.1.1:8088/
     */
    private String yarnRmAddress;
    /**
     * resourceType为K8S时生效，jobmanager的webapp地址，例：https://192.168.1.1:8081/
     */
    private String jmAddress;

    public void validate() {
        Objects.requireNonNull(resourceType, "resourceType不能为空");

        if (resourceType.equals(ResourceType.YARN) && Objects.nonNull(authType)
                && authType.equals(AuthType.KERBEROS)) {
            Objects.requireNonNull(krb5Kdc, "authType为KERBEROS时,krb5Kdc 不能为空");
            Objects.requireNonNull(krb5Realm, "authType为KERBEROS时,krb5Realm 不能为空");
            Objects.requireNonNull(keytabPath, "authType为KERBEROS时,keytabPath 不能为空");
            Objects.requireNonNull(keytabPrinciple, "authType为KERBEROS时,keytabPrinciple 不能为空");

            Objects.requireNonNull(yarnRmAddress, "authType为KERBEROS时,yarnRmAddress 不能为空");
        }

        if (resourceType.equals(ResourceType.K8S)) {
            Objects.requireNonNull(jmAddress, "resourceType为k8s时,jmAddress 不能为空");
        }

    }
}
