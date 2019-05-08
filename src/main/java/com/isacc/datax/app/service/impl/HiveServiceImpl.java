package com.isacc.datax.app.service.impl;

import java.util.List;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.HiveInfoDTO;
import com.isacc.datax.app.service.HiveService;
import com.isacc.datax.domain.entity.reader.hdfsreader.HdfsColumn;
import com.isacc.datax.infra.constant.Constants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
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
        final ApiResult<Object> successApiResult = ApiResult.initSuccess();
        List<HdfsColumn> columns = hiveInfoDTO.getColumns();
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
            successApiResult.setMessage(String.format("成功创建表%s!", hiveInfoDTO.getTableName()));
            log.info("create hive table: " + hiveInfoDTO.getTableName());
            return successApiResult;
        } catch (Exception e) {
            log.error("execute create table error: ", e);
            final ApiResult<Object> failureApiResult = ApiResult.initFailure();
            failureApiResult.setMessage(String.format("创建表%ss失败!", hiveInfoDTO.getTableName()));
            log.error("create hive table: " + hiveInfoDTO.getTableName() + "error!");
            failureApiResult.setContent(e.getMessage());
            return failureApiResult;
        }
    }

    @Override
    public ApiResult<Object> createDatabase(String databaseName) {
        final String sql = String.format("CREATE DATABASE IF NOT EXISTS %s", databaseName);
        final ApiResult<Object> successApiResult = ApiResult.initSuccess();
        try {
            jdbcTemplate.execute(sql);
            successApiResult.setMessage(String.format("成功创建数据库%s!", databaseName));
            log.info("create hive database: " + databaseName);
            return successApiResult;
        } catch (Exception e) {
            log.error("execute create hive database error", e);
            final ApiResult<Object> failureApiResult = ApiResult.initFailure();
            failureApiResult.setMessage(String.format("创建数据库%s失败!", databaseName));
            log.error("create hive database:  " + databaseName + "error!");
            failureApiResult.setContent(e.getMessage());
            return failureApiResult;
        }
    }

    @Override
    public ApiResult<Object> deleteTable(HiveInfoDTO hiveInfoDTO) {
        final String sql = String.format("DROP TABLE IF EXISTS %s%s%s", hiveInfoDTO.getDatabaseName(), Constants.Symbol.POINT, hiveInfoDTO.getTableName());
        try {
            jdbcTemplate.execute(sql);
            final ApiResult<Object> successApiResult = ApiResult.initSuccess();
            successApiResult.setMessage(String.format("成功删除表%s%s%s!", hiveInfoDTO.getDatabaseName(), Constants.Symbol.POINT, hiveInfoDTO.getTableName()));
            return successApiResult;
        } catch (Exception e) {
            log.error("execute delete hive table error", e);
            final ApiResult<Object> failureApiResult = ApiResult.initFailure();
            failureApiResult.setMessage(String.format("删除表%s%s%s失败!", hiveInfoDTO.getDatabaseName(), Constants.Symbol.POINT, hiveInfoDTO.getTableName()));
            failureApiResult.setContent(e.getMessage());
            return failureApiResult;
        }
    }

    @Override
    public ApiResult<Object> deleteDatabase(String databaseName) {
        final String sql = String.format("DROP DATABASE IF EXISTS %s", databaseName);
        try {
            jdbcTemplate.execute(sql);
            final ApiResult<Object> successApiResult = ApiResult.initSuccess();
            successApiResult.setMessage(String.format("成功删除数据库%s!", databaseName));
            log.info("delete hive database: " + databaseName);
            return successApiResult;
        } catch (Exception e) {
            log.error("execute delete hive database error", e);
            final ApiResult<Object> failureApiResult = ApiResult.initFailure();
            failureApiResult.setMessage(String.format("删除数据库%s失败!", databaseName));
            failureApiResult.setContent(e.getMessage());
            return failureApiResult;
        }
    }


}
