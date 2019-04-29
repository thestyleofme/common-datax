package com.isacc.datax.app.service.impl;

import java.util.List;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.hive.HiveInfoDTO;
import com.isacc.datax.api.dto.hive.HiveTableColumn;
import com.isacc.datax.app.service.HiveService;
import com.isacc.datax.infra.constant.Constants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;


/**
 * <p>
 * Hive Service Impl
 * </p>
 *
 * @author isacc 2019/04/28 19:41
 */
@Service
@Slf4j
public class HiveServiceImpl implements HiveService {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public HiveServiceImpl(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	@Override
	public ApiResult<Object> createTable(HiveInfoDTO hiveInfoDTO) {
		List<HiveTableColumn> columns = hiveInfoDTO.getColumns();
		final String columnSql;
		StringBuilder sb = new StringBuilder();
		columns.forEach(column -> sb.append(column.getName()).append(Constants.Symbol.SPACE).append(column.getType()).append(Constants.Symbol.COMMA));
		columnSql = sb.toString().substring(0, sb.toString().length() - 1);
		final String sql = String.format("CREATE TABLE IF NOT EXISTS %s%s%s%s%s%s%s%s ROW FORMAT DELIMITED FIELDS TERMINATED BY %s%s%s STORED AS %s",
				Constants.Symbol.BACKQUOTE, hiveInfoDTO.getDatabaseName(), Constants.Symbol.POINT, hiveInfoDTO.getTableName(), Constants.Symbol.BACKQUOTE,
				Constants.Symbol.LEFT_BRACE, columnSql, Constants.Symbol.RIGHT_BRACE,
				Constants.Symbol.SINGLE_QUOTE, hiveInfoDTO.getFieldDelimiter(), Constants.Symbol.SINGLE_QUOTE,
				hiveInfoDTO.getFileType());
		log.info("创表语句：{}", sql);
		try {
			jdbcTemplate.execute(sql);
			ApiResult.SUCCESS.setMessage(String.format("成功创建表%s!", hiveInfoDTO.getTableName()));
			return ApiResult.SUCCESS;
		} catch (DataAccessException e) {
			log.error("execute create table error {}", e);
			ApiResult.FAILURE.setMessage(String.format("创建表%ss失败!", hiveInfoDTO.getTableName()));
			ApiResult.FAILURE.setContent(e.getMessage());
			return ApiResult.FAILURE;
		}
	}

	@Override
	public ApiResult<Object> createDatabase(String databaseName) {
		final String sql = String.format("CREATE DATABASE IF NOT EXISTS %s", databaseName);
		try {
			jdbcTemplate.execute(sql);
			ApiResult.SUCCESS.setMessage(String.format("成功创建数据库%s!", databaseName));
			return ApiResult.SUCCESS;
		} catch (Exception e) {
			log.error("execute create database error {}", e);
			ApiResult.FAILURE.setMessage(String.format("成功创建数据库%s!", databaseName));
			ApiResult.FAILURE.setContent(e.getMessage());
			return ApiResult.FAILURE;
		}
	}

	@Override
	public ApiResult<Object> deleteTable(HiveInfoDTO hiveInfoDTO) {
		final String sql = String.format("DROP TABLE IF EXISTS %s%s%s", hiveInfoDTO.getDatabaseName(), Constants.Symbol.POINT, hiveInfoDTO.getTableName());
		try {
			jdbcTemplate.execute(sql);
			ApiResult.SUCCESS.setMessage(String.format("成功删除表%s%s%s!", hiveInfoDTO.getDatabaseName(), Constants.Symbol.POINT, hiveInfoDTO.getTableName()));
			return ApiResult.SUCCESS;
		} catch (Exception e) {
			log.error("execute delete table error {}", e);
			ApiResult.FAILURE.setMessage(String.format("删除表%s%s%s失败!", hiveInfoDTO.getDatabaseName(), Constants.Symbol.POINT, hiveInfoDTO.getTableName()));
			ApiResult.FAILURE.setContent(e.getMessage());
			return ApiResult.FAILURE;
		}
	}

	@Override
	public ApiResult<Object> deleteDatabase(String databaseName) {
		final String sql = String.format("DROP DATABASE IF EXISTS %s", databaseName);
		try {
			jdbcTemplate.execute(sql);
			ApiResult.SUCCESS.setMessage(String.format("成功删除数据库%s!", databaseName));
			return ApiResult.SUCCESS;
		} catch (Exception e) {
			log.error("execute delete database error {}", e);
			ApiResult.FAILURE.setMessage(String.format("删除数据库%s失败!", databaseName));
			ApiResult.FAILURE.setContent(e.getMessage());
			return ApiResult.FAILURE;
		}
	}


}
