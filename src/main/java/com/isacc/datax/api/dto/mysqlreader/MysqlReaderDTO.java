package com.isacc.datax.api.dto.mysqlreader;


import java.util.List;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

/**
 * <p>
 * DataX Mysql插件的parameter封装
 * </p>
 *
 * @author lei.xie03@hand-china.com 2019/04/28 10:34
 */
@Builder
@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MysqlReaderDTO {

	/**
	 * 数据源的用户名
	 */
	@NotBlank
	private String username;
	/**
	 * 数据源指定用户名的密码
	 */
	@NotBlank
	private String password;
	/**
	 * 所配置的表中需要同步的列名集合
	 * 全选时值为：['*']
	 */
	@NotEmpty
	private List<String> column;
	/**
	 * 使用splitPk代表的字段进行数据分片，仅支持整形数据切分，不支持浮点、字符串、日期等其他类型
	 */
	private String splitPk;
	/**
	 * mysql连接信息
	 */
	@NotEmpty
	private List<MysqlReaderConnection> connection;
	/**
	 * 筛选条件，MysqlReader根据指定的column、table、where条件拼接SQL，并根据这个SQL进行数据抽取
	 */
	private String where;
}
