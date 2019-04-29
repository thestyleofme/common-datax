package com.isacc.datax.infra.mapper;

import com.baomidou.dynamic.datasource.annotation.DS;
import org.apache.ibatis.annotations.Select;

/**
 * <p>
 * Mysql Simple Mapper
 * </p>
 *
 * @author isacc 2019/04/29 20:24
 */
@DS("mysql")
public interface MysqlSimpleMapper {

	/**
	 * 是否存在数据库
	 *
	 * @param databaseName Mysql数据库名称
	 * @return java.lang.Integer
	 * @author isacc 2019-04-29 20:09
	 */
	@Select("SELECT COUNT(*) FROM information_schema.SCHEMATA " +
			"WHERE SCHEMA_NAME = #{databaseName}")
	Integer databaseIsExist(String databaseName);

	/**
	 * 判断指定数据库下指定表是否存在
	 *
	 * @param databaseName Mysql数据库名称
	 * @param tableName    Mysql表名称
	 * @return java.lang.Integer
	 * @author isacc 2019-04-29 21:20
	 */
	@Select("SELECT count(*) FROM information_schema.TABLES " +
			"WHERE table_schema=#{databaseName} and table_name=#{tableName}")
	Integer tableIsExist(String databaseName, String tableName);

}
