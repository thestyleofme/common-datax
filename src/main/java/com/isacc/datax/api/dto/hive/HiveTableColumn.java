package com.isacc.datax.api.dto.hive;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

/**
 * <p>
 * Hive表的字段
 * </p>
 *
 * @author isacc 2019/04/28 19:31
 */
@Builder
@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class HiveTableColumn {

	/**
	 * HIve表字段的类型
	 */
	private String type;
	/**
	 * Hive表字段名
	 */
	private String name;

}
