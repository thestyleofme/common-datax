package com.isacc.datax.infra.mapper;

import java.util.List;
import java.util.Map;

import com.baomidou.dynamic.datasource.annotation.DS;
import org.apache.ibatis.annotations.ResultMap;
import org.apache.ibatis.annotations.ResultType;
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

	/**
	 * 查询Hive所有数据库
	 *
	 * @return java.util.List<java.util.Map < java.lang.String, java.lang.Object>>
	 * @author isacc 2019-04-30 10:48
	 */
	@DS("hivemetadata")
	@Select("select DB_ID,`DESC`,DB_LOCATION_URI,`NAME`,OWNER_NAME,OWNER_TYPE from dbs")
	@ResultType(Map.class)
	List<Map<String, Object>> allHiveDatabases();

	/**
	 * 查询指定Hive数据库下所有表
	 *
	 * @param dbId Hive数据库ID
	 * @return java.util.List<java.util.Map < java.lang.String, java.lang.Object>>
	 * @author isacc 2019-04-30 10:54
	 */
	@DS("hivemetadata")
	@Select("SELECT " +
			"TBL_ID,CREATE_TIME,DB_ID LAST_ACCESS_TIME,OWNER RETENTION,SD_ID,TBL_NAME,TBL_TYPE,VIEW_EXPANDED_TEXT,VIEW_ORIGINAL_TEXT,IS_REWRITE_ENABLED " +
			"FROM " +
			"tbls " +
			"WHERE " +
			"DB_ID = #{dbId}")
	@ResultType(Map.class)
	List<Map<String, Object>> allHiveTableByDatabase(Long dbId);

}
