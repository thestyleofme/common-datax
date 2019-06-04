package com.isacc.datax.domain.entity.reader.mysqlreader;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.isacc.datax.domain.entity.reader.BaseReader;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import lombok.*;

import java.util.List;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;

/**
 * <p>
 * DataX Mysql插件的parameter封装
 * </p>
 *
 * @author isacc 2019/04/28 10:34
 */
@Builder
@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MysqlReader extends BaseReader {

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
	private List<ReaderConnection> connection;
	/**
	 * 筛选条件，MysqlReader根据指定的column、table、where条件拼接SQL，并根据这个SQL进行数据抽取
	 */
	private String where;
}
