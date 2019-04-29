package com.isacc.datax.api.dto;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.isacc.datax.domain.entity.hive.HiveTableColumn;
import lombok.*;

/**
 * <p>
 * Hive信息
 * </p>
 *
 * @author ISACC 2019/04/28 19:33
 */
@Builder
@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class HiveInfoDTO {

	/**
	 * Hive数据库
	 */
	private String databaseName;
	/**
	 * Hive表名
	 */
	private String tableName;
	/**
	 * Hive表字段
	 */
	private List<HiveTableColumn> columns;
	/**
	 * 字段分割符
	 */
	private String fieldDelimiter;
	/**
	 * 文件的类型，目前只支持用户配置为"text"、"orc"、"rc"、"seq"、"csv"
	 */
	private String fileType;

}
