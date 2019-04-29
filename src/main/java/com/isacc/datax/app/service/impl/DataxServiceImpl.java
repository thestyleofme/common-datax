package com.isacc.datax.app.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.Mysql2HiveDTO;
import com.isacc.datax.app.service.DataxService;
import com.isacc.datax.domain.entity.reader.mysqlreader.MysqlReaderConnection;
import com.isacc.datax.infra.mapper.MysqlSimpleMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * <p>
 * DataX Service Impl
 * </p>
 *
 * @author isacc 2019/04/29 17:06
 */
@SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
@Service
@Slf4j
public class DataxServiceImpl implements DataxService {

	private final MysqlSimpleMapper mysqlSimpleMapper;

	@Autowired
	public DataxServiceImpl(MysqlSimpleMapper mysqlSimpleMapper) {
		this.mysqlSimpleMapper = mysqlSimpleMapper;
	}

	@Override
	public ApiResult<Object> mysql2Hive(Mysql2HiveDTO mysql2HiveDTO) {
		// 判断mysql的数据库、表是否存在
		ApiResult<Object> result = this.databaseIsExist(mysql2HiveDTO);
		if (!result.getResult()) {
			return result;
		}
		// 判断hive的数据库、表是否存在，无则创建
		// 创建datax job json文件
		// 远程执行python进行导数
		return ApiResult.SUCCESS;
	}

	/**
	 * 校验mysql的数据库、表是否存在
	 *
	 * @param mysql2HiveDTO Mysql2HiveDTO
	 * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
	 * @author isacc 2019-04-29 21:11
	 */
	private ApiResult<Object> databaseIsExist(Mysql2HiveDTO mysql2HiveDTO) {
		final List<String> databaseNameList = new ArrayList<>(5);
		final List<String> tableList = new ArrayList<>(10);
		mysql2HiveDTO.getReader().getConnection().stream().map(MysqlReaderConnection::getJdbcUrl).forEach(jdbcUrls -> jdbcUrls.forEach(url ->
				databaseNameList.add(url.substring(url.lastIndexOf('/') + 1, !url.contains("?") ? url.length() : url.indexOf('?')))
		));
		List<String> collect = databaseNameList.stream().distinct().collect(Collectors.toList());
		if (collect.size() != 1) {
			ApiResult.FAILURE.setMessage("mysqlreader jdbcUrl database has too many!");
			ApiResult.FAILURE.setContent("databases: " + collect);
			return ApiResult.FAILURE;
		}
		String databaseName = collect.get(0);
		if (mysqlSimpleMapper.databaseIsExist(databaseName) == 0) {
			ApiResult.FAILURE.setMessage("mysqlreader jdbcUrl database is not exist!");
			ApiResult.FAILURE.setContent("database: " + databaseName);
			return ApiResult.FAILURE;
		}
		mysql2HiveDTO.getReader().getConnection().stream().map(MysqlReaderConnection::getTable).forEach(tableList::addAll);
		final List<String> notExistTables = new ArrayList<>(10);
		tableList.forEach(table -> {
			if (mysqlSimpleMapper.tableIsExist(databaseName, table) == 0) {
				notExistTables.add(table);
			}
		});
		if (!notExistTables.isEmpty()) {
			ApiResult.FAILURE.setMessage("mysqlreader table does not exist!");
			ApiResult.FAILURE.setContent("table not exist: " + notExistTables);
			return ApiResult.FAILURE;
		}
		return ApiResult.SUCCESS;
	}


}
