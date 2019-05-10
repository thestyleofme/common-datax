package com.isacc.datax.domain.entity.datax;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

/**
 * <p>
 * mysqlReader获取mysql的信息
 * </P>
 *
 * @author isacc 2019/05/10 16:29
 */
@Builder
@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MysqlInfo {

    /**
     * 数据库名
     */
    private String databaseName;
    /**
     * 库下的表集合
     */
    private List<String> tableList;
}
