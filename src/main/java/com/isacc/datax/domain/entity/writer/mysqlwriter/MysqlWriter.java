package com.isacc.datax.domain.entity.writer.mysqlwriter;


import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.isacc.datax.domain.entity.writer.BaseWriter;
import lombok.*;

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
public class MysqlWriter extends BaseWriter {

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
	private String[] column;
	/**
	 * Mysql连接信息
	 */
	@NotNull
	private MysqlWriterConnection connection;
	/**
	 * DataX在获取Mysql连接时，执行session指定的SQL语句，修改当前connection session属性
	 */
	private String session;
	/**
	 * 写入数据到目的表前，会先执行这里的标准语句
	 */
	private String preSql;
	/**
	 * 写入数据到目的表后，会执行这里的标准语句
	 */
	private String postSql;
	/**
	 * 控制写入数据到目标表采用 insert into 或者 replace into 或者 ON DUPLICATE KEY UPDATE 语句
	 * 所有选项：insert/replace/update
	 * 默认值：insert
	 */
	@NotBlank
	private String writeMode;
	/**
	 * 一次性批量提交的记录数大小，该值可以极大减少DataX与Mysql的网络交互次数，并提升整体吞吐量
	 * 但是该值设置过大可能会造成DataX运行进程OOM情况。
	 * 默认值：1024
	 */
	private Long batchSize;


}
