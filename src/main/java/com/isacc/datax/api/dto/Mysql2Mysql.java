package com.isacc.datax.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.isacc.datax.domain.entity.datax.BaseDatax;
import com.isacc.datax.domain.entity.reader.mysqlreader.MysqlReader;
import com.isacc.datax.domain.entity.writer.mysqlwriter.MysqlWriter;
import lombok.*;

/**
 * <p>
 * description
 * </P>
 *
 * @author isacc 2019/05/22 11:35
 */
@Builder
@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Mysql2Mysql extends BaseDatax {
    /**
     * DataX MysqlReader
     */
    private MysqlReader reader;
    /**
     * DataX MysqlWriter
     */
    private MysqlWriter writer;
}
