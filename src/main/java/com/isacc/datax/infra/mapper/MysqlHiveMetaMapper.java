package com.isacc.datax.infra.mapper;

import java.util.List;
import java.util.Map;

import com.baomidou.dynamic.datasource.annotation.DS;
import org.apache.ibatis.annotations.ResultType;
import org.apache.ibatis.annotations.Select;

/**
 * description
 *
 * @author isacc 2019/06/03 21:41
 */
@DS("mysql_hivemeta")
public interface MysqlHiveMetaMapper {

    /**
     * 查询Hive所有数据库
     *
     * @return java.util.List<java.util.Map < java.lang.String, java.lang.Object>>
     * @author isacc 2019-04-30 10:48
     */
    @Select("select DB_ID,`DESC`,DB_LOCATION_URI,`NAME`,OWNER_NAME,OWNER_TYPE from dbs")
    @ResultType(Map.class)
    List<Map<String, Object>> allHiveDatabases();

    /**
     * 根据Hive数据库名称查看是否存在该数据库
     *
     * @param hiveDbName hive db name
     * @return java.util.Map<java.lang.String, java.lang.Object>
     * @author isacc 2019-05-07 15:54
     */
    @Select("select DB_ID,`DESC`,DB_LOCATION_URI,`NAME`,OWNER_NAME,OWNER_TYPE from dbs " +
            "where `NAME` = #{hiveDbName}")
    @ResultType(Map.class)
    Map<String, Object> hiveDbIsExist(String hiveDbName);

    /**
     * 根据Hive数据库ID查看是否存在该表
     *
     * @param dbId        hive数据库id
     * @param hiveTblName hive数据表名称
     * @return java.util.Map<java.lang.String, java.lang.Object>
     * @author isacc 2019-05-07 16:08
     */
    @Select("SELECT " +
            "TBL_ID,CREATE_TIME,DB_ID LAST_ACCESS_TIME,OWNER RETENTION,SD_ID,TBL_NAME,TBL_TYPE,VIEW_EXPANDED_TEXT,VIEW_ORIGINAL_TEXT,IS_REWRITE_ENABLED " +
            "FROM " +
            "tbls " +
            "WHERE " +
            "DB_ID = #{dbId} " +
            "AND TBL_NAME = #{hiveTblName}")
    @ResultType(Map.class)
    Map<String, Object> hiveTblIsExist(Long dbId, String hiveTblName);

    /**
     * 查询指定Hive数据库下所有表
     *
     * @param dbId Hive数据库ID
     * @return java.util.List<java.util.Map < java.lang.String, java.lang.Object>>
     * @author isacc 2019-04-30 10:54
     */

    @Select("SELECT " +
            "TBL_ID,CREATE_TIME,DB_ID LAST_ACCESS_TIME,OWNER RETENTION,SD_ID,TBL_NAME,TBL_TYPE,VIEW_EXPANDED_TEXT,VIEW_ORIGINAL_TEXT,IS_REWRITE_ENABLED " +
            "FROM " +
            "tbls " +
            "WHERE " +
            "DB_ID = #{dbId}")
    @ResultType(Map.class)
    List<Map<String, Object>> allHiveTableByDatabase(Long dbId);

}
