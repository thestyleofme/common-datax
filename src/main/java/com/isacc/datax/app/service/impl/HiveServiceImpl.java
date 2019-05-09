package com.isacc.datax.app.service.impl;

import java.util.List;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.HiveInfoDTO;
import com.isacc.datax.app.service.HiveService;
import com.isacc.datax.domain.entity.datax.HivePartition;
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
        StringBuilder sb = new StringBuilder();
        columns.forEach(column -> sb.append(column.getName()).append(Constants.Symbol.SPACE).append(column.getType()).append(Constants.Symbol.COMMA));
        final String columnSql = sb.toString().substring(0, sb.toString().length() - 1);
        final String sql;
        // 检验是否是分区表
        List<HivePartition> partitionList = hiveInfoDTO.getPartitionList();
        if (partitionList.isEmpty()) {
            // 无分区
            sql = String.format("CREATE TABLE IF NOT EXISTS `%s.%s`(%s) ROW FORMAT DELIMITED FIELDS TERMINATED BY \"%s\" STORED AS %s",
                    hiveInfoDTO.getDatabaseName(), hiveInfoDTO.getTableName(),
                    columnSql, hiveInfoDTO.getFieldDelimiter(),
                    hiveInfoDTO.getFileType());
        } else {
            // 有分区
            StringBuilder dtSb = new StringBuilder();
            partitionList.forEach(partition -> dtSb.append(partition.getName()).append(Constants.Symbol.SPACE).append(partition.getType()).append(Constants.Symbol.COMMA));
            final String partitionSql = dtSb.toString().substring(0, dtSb.toString().length() - 1);
            sql = String.format("CREATE TABLE IF NOT EXISTS `%s.%s`(%s) PARTITIONED BY (%s) ROW FORMAT DELIMITED FIELDS TERMINATED BY \"%s\" STORED AS %s",
                    hiveInfoDTO.getDatabaseName(), hiveInfoDTO.getTableName(),
                    columnSql, partitionSql, hiveInfoDTO.getFieldDelimiter(),
                    hiveInfoDTO.getFileType());
        }
        log.info("创表语句：{}", sql);
        try {
            jdbcTemplate.execute(sql);
            successApiResult.setMessage(String.format("成功创建表%s!", hiveInfoDTO.getTableName()));
            log.info("create hive table: {}.{}", hiveInfoDTO.getDatabaseName(), hiveInfoDTO.getTableName());
            return successApiResult;
        } catch (Exception e) {
            log.error("execute create table error: ", e);
            final ApiResult<Object> failureApiResult = ApiResult.initFailure();
            failureApiResult.setMessage(String.format("创建表%s失败!", hiveInfoDTO.getTableName()));
            log.error("create hive table: {} error!", hiveInfoDTO.getTableName());
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
            log.info("create hive database: {}!", databaseName);
            return successApiResult;
        } catch (Exception e) {
            log.error("execute create hive database error", e);
            final ApiResult<Object> failureApiResult = ApiResult.initFailure();
            failureApiResult.setMessage(String.format("创建数据库%s失败!", databaseName));
            failureApiResult.setContent(e.getMessage());
            return failureApiResult;
        }
    }

    @Override
    public ApiResult<Object> addPartition(HiveInfoDTO hiveInfoDTO) {
        List<HivePartition> partitionList = hiveInfoDTO.getPartitionList();
        StringBuilder sb = new StringBuilder();
        partitionList.forEach(partition -> sb.append(partition.getName()).append(Constants.Symbol.EQUAL).append(partition.getValue()).append(Constants.Symbol.COMMA));
        final String partitionInfoSql = sb.toString().substring(0, sb.toString().length() - 1);
        final String sql = String.format("ALTER TABLE `%s.%s` ADD PARTITION(%s)",
                hiveInfoDTO.getDatabaseName(), hiveInfoDTO.getTableName(), partitionInfoSql
        );
        final ApiResult<Object> successApiResult = ApiResult.initSuccess();
        try {
            jdbcTemplate.execute(sql);
            successApiResult.setMessage(String.format("成功创建分区%s!", partitionInfoSql));
            log.info("create hive table partition: {}!", partitionInfoSql);
            return successApiResult;
        } catch (Exception e) {
            log.error("execute create hive table partition error", e);
            final ApiResult<Object> failureApiResult = ApiResult.initFailure();
            failureApiResult.setMessage(String.format("创建分区%s失败!", partitionInfoSql));
            failureApiResult.setContent(e.getMessage());
            return failureApiResult;
        }
    }

    @Override
    public ApiResult<Object> deleteTable(HiveInfoDTO hiveInfoDTO) {
        final String sql = String.format("DROP TABLE IF EXISTS %s.%s", hiveInfoDTO.getDatabaseName(), hiveInfoDTO.getTableName());
        try {
            jdbcTemplate.execute(sql);
            log.info("delete hive table: {}.{}!", hiveInfoDTO.getDatabaseName(), hiveInfoDTO.getTableName());
            final ApiResult<Object> successApiResult = ApiResult.initSuccess();
            successApiResult.setMessage(String.format("成功删除表%s.%s!", hiveInfoDTO.getDatabaseName(), hiveInfoDTO.getTableName()));
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
            log.info("delete hive database: {} !", databaseName);
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
