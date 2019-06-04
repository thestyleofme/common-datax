package com.isacc.datax.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.isacc.datax.domain.entity.datax.BaseDatax;
import com.isacc.datax.domain.entity.reader.mysqlreader.MysqlReader;
import com.isacc.datax.domain.entity.writer.hdfswiter.HdfsWriter;
import lombok.*;

/**
 * <p>
 * DataX封装
 * </p>
 *
 * @author isacc 2019/04/29 13:44
 */
@Builder
@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Mysql2Hive extends BaseDatax {

    /**
     * DataX MysqlReader
     */
    private MysqlReader reader;
    /**
     * DataX HdfsWriter
     */
    private HdfsWriter writer;

}
