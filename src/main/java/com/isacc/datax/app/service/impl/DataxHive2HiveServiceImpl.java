package com.isacc.datax.app.service.impl;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.Hive2HiveDTO;
import com.isacc.datax.app.service.DataxHive2HiveService;
import com.isacc.datax.domain.repository.MysqlRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * <p>
 * DataX Hive2Hive Service Impl
 * </p>
 *
 * @author isacc 2019/05/07 14:19
 */
@Service
@Slf4j
public class DataxHive2HiveServiceImpl implements DataxHive2HiveService {

	private final MysqlRepository mysqlRepository;

	@Autowired
	public DataxHive2HiveServiceImpl(MysqlRepository mysqlRepository) {
		this.mysqlRepository = mysqlRepository;
	}

	@Override
	public ApiResult<Object> hive2hive(Hive2HiveDTO hive2HiveDTO) {
		// 校验reader中hive库/表是否存在，不存在则返回
		final ApiResult<Object> hiveDbAndTableIsExistApiResult = mysqlRepository.hiveDbAndTableIsExist(hive2HiveDTO);
		if (!hiveDbAndTableIsExistApiResult.getResult()) {
			return hiveDbAndTableIsExistApiResult;
		}
		// 校验 fileType fieldDelimiter
		return ApiResult.initSuccess();
	}
}
