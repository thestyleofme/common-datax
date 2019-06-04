package com.isacc.datax.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.isacc.datax.domain.entity.datax.BaseDatax;
import com.isacc.datax.domain.entity.reader.mysqlreader.MysqlReader;
import com.isacc.datax.domain.entity.writer.oraclewriter.OracleWriter;
import lombok.*;

/**
 * Mysql2Oracle
 *
 * @author isacc 2019/05/29 15:02
 */
@Builder
@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Mysql2Oracle extends BaseDatax {

    /**
     * mysqlreader
     */
    private MysqlReader reader;
    /**
     * oraclewriter
     */
    private OracleWriter writer;

}
