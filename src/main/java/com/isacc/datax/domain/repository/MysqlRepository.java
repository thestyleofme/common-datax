package com.isacc.datax.domain.repository;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.Hive2Hive;

/**
 * <p>
 * Mysql Repository
 * </p>
 *
 * @author isacc 2019/04/29 19:46
 */
public interface MysqlRepository {

	/**
	 * 校验Hive数据库是否存在 reader and writer
	 *
	 * @param hive2Hive Hive2Hive
	 * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
	 * @author isacc 2019-05-07 15:22
	 */
	ApiResult<Object> checkHiveDbAndTable(Hive2Hive hive2Hive);

	/**
	 * 校验Hive数据库是否存在 writer
	 *
	 * @param hive2Hive Hive2Hive
	 * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
	 * @author isacc 2019-05-07 15:22
	 */
	ApiResult<Object> checkWriterHiveDbAndTable(Hive2Hive hive2Hive);

}
