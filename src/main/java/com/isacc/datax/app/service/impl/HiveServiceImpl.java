package com.isacc.datax.app.service.impl;

import java.util.List;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.hive.HiveInfoDTO;
import com.isacc.datax.api.dto.hive.HiveTableColumn;
import com.isacc.datax.app.service.HiveService;
import com.isacc.datax.infra.constant.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.httpclient.HttpStatus;
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
	public ApiResult<String> createTable(HiveInfoDTO hiveInfoDTO) {
		ApiResult<String> result = new ApiResult<>();
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
			result.setResult(true);
			result.setCode(HttpStatus.SC_OK);
			result.setMessage(String.format("成功创建表%s!", hiveInfoDTO.getTableName()));
		} catch (DataAccessException e) {
			log.info("execute create table error", e);
			result.setResult(false);
			result.setCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
			result.setMessage(String.format("创建表%ss失败!", hiveInfoDTO.getTableName()));
			result.setContent(e.getMessage());
		}
		return result;
	}
}
