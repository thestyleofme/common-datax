package com.isacc.datax.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.isacc.datax.domain.entity.datax.BaseDatax;
import com.isacc.datax.domain.entity.reader.hdfsreader.HdfsReader;
import com.isacc.datax.domain.entity.writer.mysqlwriter.MysqlWriter;
import lombok.*;

/**
 * <p>
 * Hive2HiveDTO
 * </p>
 *
 * @author isacc 2019/05/07 14:12
 */
@Builder
@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Hive2Mysql extends BaseDatax {
    /**
     * DataX HdfsReader
     */
    private HdfsReader reader;
    /**
     * DataX MysqlWriter
     */
    private MysqlWriter writer;
}
