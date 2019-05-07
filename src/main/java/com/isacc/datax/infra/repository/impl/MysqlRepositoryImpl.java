package com.isacc.datax.infra.repository.impl;

import java.util.Map;
import java.util.Objects;
import javax.validation.constraints.NotBlank;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.Hive2HiveDTO;
import com.isacc.datax.domain.repository.MysqlRepository;
import com.isacc.datax.infra.mapper.MysqlSimpleMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * <p>
 * Mysql Repository Impl
 * </p>
 *
 * @author isacc 2019/04/29 19:49
 */
@SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
@Component
public class MysqlRepositoryImpl implements MysqlRepository {

	private final MysqlSimpleMapper mysqlSimpleMapper;

	@Autowired
	public MysqlRepositoryImpl(MysqlSimpleMapper mysqlSimpleMapper) {
		this.mysqlSimpleMapper = mysqlSimpleMapper;
	}

	@Override
	public ApiResult<Object> hiveDbAndTableIsExist(Hive2HiveDTO hive2HiveDTO) {
		@NotBlank final String readerPath = hive2HiveDTO.getReader().getPath();
		String hivePath = readerPath.substring(0, readerPath.lastIndexOf('/'));
		String hiveDbName = hivePath.substring(hivePath.lastIndexOf('/') + 1, hivePath.indexOf('.'));
		final Map<String, Object> hiveDbInfoMap = mysqlSimpleMapper.hiveDbIsExist(hiveDbName);
		if (Objects.isNull(hiveDbInfoMap)) {
			final ApiResult<Object> failureApiResult = ApiResult.initFailure();
			failureApiResult.setMessage(String.format("hdfsreader中path路径错误，不存在该hive数据库：%s!", hiveDbName));
			return failureApiResult;
		}
		String hiveTblName = readerPath.substring(readerPath.lastIndexOf('/') + 1);
		final Long dbId = Long.valueOf(String.valueOf(hiveDbInfoMap.get("DB_ID")));
		final Map<String, Object> hiveTblInfoMap = mysqlSimpleMapper.hiveTblIsExist(dbId, hiveTblName);
		if (Objects.isNull(hiveTblInfoMap)) {
			final ApiResult<Object> failureApiResult = ApiResult.initFailure();
			failureApiResult.setMessage(String.format("hdfsreader中path路径错误，%s数据库下不存在表：%s!", hiveDbName, hiveTblName));
			return failureApiResult;
		}
		return ApiResult.initSuccess();
	}

}
