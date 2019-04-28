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
	ApiResult<String> createTable(HiveInfoDTO hiveInfoDTO);
}
