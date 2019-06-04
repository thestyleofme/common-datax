package com.isacc.datax.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.isacc.datax.domain.entity.datax.BaseDatax;
import com.isacc.datax.domain.entity.reader.hdfsreader.HdfsReader;
import com.isacc.datax.domain.entity.writer.oraclewriter.OracleWriter;
import lombok.*;

/**
 * description
 *
 * @author isacc 2019/05/29 14:31
 */
@Builder
@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Hive2Oracle extends BaseDatax {

    /**
     * hdfsreader
     */
    private HdfsReader reader;
    /**
     * oraclewriter
     */
    private OracleWriter writer;

}
