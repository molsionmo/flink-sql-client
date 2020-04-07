package com.github.mxb.flink.sql.parser;

import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

@Slf4j
public class SqlListsTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void getSQLList() {
        String sql = "create table dbo_CouponOrigin (\n" +
                "    messageKey varbinary,\n" +
                "    `message` varbinary,\n" +
                "    PRIMARY KEY (messageKey)\n" +
                ") with (\n" +
                "    type = 'kafka011plus',\n" +
                "    topic = 'licai_test_sqlserver.ppmoney_whtr_data_QY.dbo.CouponOrigin',\n" +
                "    bootstrap.servers = '192.168.102.85:9092,192.168.102.86:9092,192.168.102.87:9092'\n" +
                ");\n" +
                "create table dbo_Whir_Member_Member (\n" +
                "    messageKey varbinary,\n" +
                "    `message` varbinary,\n" +
                "    PRIMARY KEY (messageKey)\n" +
                ") with (\n" +
                "    type = 'kafka011plus',\n" +
                "    topic = 'licai_test_sqlserver.ppmoney_whtr_data_QY.dbo.Whir_Member_Member',\n" +
                "    bootstrap.servers = '192.168.102.85:9092,192.168.102.86:9092,192.168.102.87:9092'\n" +
                ");\n" +
                "create table cdcSource (\n" +
                "    key varchar,\n" +
                "    ums varchar,\n" +
                "    `table` varchar\n" +
                ") with (\n" +
                "    type = 'SQLSERVER_CDC',\n" +
                "    `name` = 'licai_test_sqlserver.ppmoney_whtr_data_QY',\n" +
                "    `dbHostName` = '192.168.107.18',\n" +
                "    `dbPort` = '1433',\n" +
                "    `dbUser` = 'u_wangjianwen',\n" +
                "    `dbPassword` = 'W6sS69Eb4HahuJwG3ugg',\n" +
                "    `dbName` = 'ppmoney_whtr_data_QY',\n" +
                "    `dbServerName` = 'SQLSERVER',\n" +
                "    `table` = 'dbo.CouponOrigin,dbo.Whir_Member_Member'\n" +
                ");\n" +
                "create view cdcView(key,ums,`table`) as select key,ums,`table` from cdcSource;\n" +
                "insert into dbo_CouponOrigin\n" +
                "select cast(key as varbinary),cast(ums as varbinary)\n" +
                "from cdcView\n" +
                "where `table` in ('dbo.CouponOrigin');\n" +
                "insert into dbo_Whir_Member_Member\n" +
                "select cast(key as varbinary),cast(ums as varbinary)\n" +
                "from cdcView\n" +
                "where `table` in ('dbo.Whir_Member_Member');";

        List<SqlInfo> sqlInfos = SqlLists.getSQLList(sql);

        log.info("{}", sqlInfos);
    }
}