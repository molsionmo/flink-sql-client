package com.github.mxb.flink.sql.util;

import com.github.mxb.flink.sql.parser.FlinkSqlParserUtil;
import com.github.mxb.flink.sql.parser.SqlNodeInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

@Slf4j
public class FlinkSqlParserUtilTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void parseSqlContext() throws SqlParseException {
        String sql = "create table dbo_CouponOrigin (\n" +
                "    messageKey varbinary,\n" +
                "    `message` varbinary,\n" +
                "    PRIMARY KEY (messageKey)\n" +
                ") with (\n" +
                "    'connector.type' = 'kafka',\n" +
                "    'connector.version' = '0.11',\n" +
                "    connector.topic = 'topic_name',\n" +
                "    'connector.properties.group.id' = 'testGroup',\n" +
                "    'connector.startup-mode' = 'earliest-offset',\n" +
                "    connector.properties.bootstrap.servers = 'localhost:9092',\n" +
                "    'format.type' = 'json'\n" +
                ");\n" +
                "create table dbo_Whir_Member_Member (\n" +
                "    messageKey varbinary,\n" +
                "    `message` varbinary,\n" +
                "    PRIMARY KEY (messageKey)\n" +
                ") with (\n" +
                "    'connector.type' = 'kafka',\n" +
                "    'connector.version' = '0.11',\n" +
                "    connector.topic = 'topic_name',\n" +
                "    'connector.properties.group.id' = 'testGroup',\n" +
                "    'connector.startup-mode' = 'earliest-offset',\n" +
                "    connector.properties.bootstrap.servers = 'localhost:9092',\n" +
                "    'format.type' = 'json'\n" +
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

        List<SqlNodeInfo> sqlNodeList = FlinkSqlParserUtil.parseSqlContext(sql);
        log.info("{}", sqlNodeList);
    }
}