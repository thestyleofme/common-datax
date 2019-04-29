package com.isacc.datax.app.service;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.hive.HiveInfoDTO;

/**
 * <p>
 * Hive CURD
 * </p>
 *
 * @author isacc 2019/04/28 19:38
 */
public interface HiveService {

	/**
	 * 创建Hive表
	 *
	 * @param hiveInfoDTO HiveInfoDTO
	 * @return com.isacc.datax.api.dto.ApiResult<com.isacc.datax.api.dto.hive.HiveInfoDTO>
	 * @author isacc 2019-04-28 19:44
	 */
	ApiResult<Object> createTable(HiveInfoDTO hiveInfoDTO);

	/**
	 * 新建Hive数据库
	 *
	 * @param databaseName 数据库名称
	 * @return com.isacc.datax.api.dto.ApiResult<java.lang.String>
	 * @author HP_USER 2019-04-29 9:58
	 */
	ApiResult<Object> createDatabase(String databaseName);

	/**
	 * 删除Hive表
	 *
	 * @param hiveInfoDTO HiveInfoDTO
	 * @return com.isacc.datax.api.dto.ApiResult<java.lang.String>
	 * @author isacc 2019-04-29 11:28
	 */
	ApiResult<Object> deleteTable(HiveInfoDTO hiveInfoDTO);

	/**
	 * 删除Hive数据库
	 *
	 * @param databaseName 数据库名称
	 * @return com.isacc.datax.api.dto.ApiResult<java.lang.String>
	 * @author HP_USER 2019-04-29 9:58
	 */
	ApiResult<Object> deleteDatabase(String databaseName);
}
