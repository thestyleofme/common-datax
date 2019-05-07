package com.isacc.datax.domain.repository;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.Hive2HiveDTO;

/**
 * <p>
 * Mysql Repository
 * </p>
 *
 * @author isacc 2019/04/29 19:46
 */
public interface MysqlRepository {

	/**
	 * 校验Hive数据库是否存在
	 *
	 * @param hive2HiveDTO Hive2HiveDTO
	 * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
	 * @author isacc 2019-05-07 15:22
	 */
	ApiResult<Object> hiveDbAndTableIsExist(Hive2HiveDTO hive2HiveDTO);

}
