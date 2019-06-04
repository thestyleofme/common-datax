package com.isacc.datax.domain.entity.reader.mysqlreader;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.util.List;
import javax.validation.constraints.NotEmpty;

/**
 * <p>
 * DataX Mysql,Oracle插件的connection封装
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
public class ReaderConnection {

	/**
	 * 需要同步的表，支持同一schema下多张表同时抽取
	 */
	@NotEmpty
	private List<String> table;
	/**
	 * 支持一个库填写多个连接地址，依次探测ip的可连接性，直到选择一个合法的IP
	 */
	@NotEmpty
	private List<String> jdbcUrl;
	/**
	 * 当用户配置querySql时，reader直接忽略table、column、where条件的配置
	 */
	@NotEmpty
	private List<String> querySql;

}
