package com.isacc.datax.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.isacc.datax.domain.entity.datax.BaseDatax;
import com.isacc.datax.domain.entity.reader.oraclereader.OracleReader;
import com.isacc.datax.domain.entity.writer.hdfswiter.HdfsWriter;
import lombok.*;

/**
 * description
 *
 * @author isacc 2019/05/28 11:28
 */
@Builder
@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Oracle2Hive extends BaseDatax {

    /**
     * oraclereader
     */
    private OracleReader reader;
    /**
     * hdfswriter
     */
    private HdfsWriter writer;
}
