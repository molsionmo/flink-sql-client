package com.github.mxb.flink.sql.model.run;

import lombok.Builder;
import lombok.Data;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * program job result
 *
 * @author moxianbin
 * @since 2019/5/27 14:26
 */
@Builder
@Data
public class ProgramResultDescriptor {

    private String jobId;

    private String ip;

    private int port;

    private boolean isMaterialized;

    private List<Row> resultRows;

}
