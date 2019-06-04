package com.isacc.datax.app.service.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.DataxSyncDTO;
import com.isacc.datax.api.dto.Oracle2Mysql;
import com.isacc.datax.app.service.DataxHandler;
import com.isacc.datax.infra.annotation.DataxHandlerType;
import com.isacc.datax.infra.config.DataxProperties;
import com.isacc.datax.infra.constant.DataxHandlerTypeConstants;
import com.isacc.datax.infra.constant.DataxParameterConstants;
import com.isacc.datax.infra.util.GenerateDataModelUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * description
 *
 * @author isacc 2019/05/29 11:49
 */
@Service
@Slf4j
@DataxHandlerType(DataxHandlerTypeConstants.ORACLE2MYSQL)
public class Oracle2MysqlHandler  extends BaseDataxServiceImpl implements DataxHandler {

    private final DataxProperties dataxProperties;

    public Oracle2MysqlHandler(DataxProperties dataxProperties) {
        this.dataxProperties = dataxProperties;
    }

    @Override
    public ApiResult<Object> handle(DataxSyncDTO dataxSyncDTO) {
        return this.oracle2Mysql(dataxSyncDTO);
    }

    private ApiResult<Object> oracle2Mysql(DataxSyncDTO dataxSyncDTO) {
        final Oracle2Mysql oracle2Mysql = dataxSyncDTO.getOracle2Mysql();
        ApiResult<Object> failureResult = ApiResult.initFailure();
        if (!Optional.ofNullable(oracle2Mysql).isPresent()) {
            failureResult.setMessage("DataxSyncDTO.oracle2Mysql is null!");
            return failureResult;
        }
        // 判断是where还是querySql
        ApiResult<Object> generateJsonFileAndUploadResult;
        String jsonFileName = dataxSyncDTO.getJsonFileName();
        if (Optional.ofNullable(oracle2Mysql.getReader().getWhere()).isPresent()) {
            // where模式
            final String whereTemplate = dataxProperties.getOracle2Mysql().getWhereTemplate();
            Map<String, Object> dataModelWhere = this.generateDataModelOracle2MysqlWhere(oracle2Mysql);
            generateJsonFileAndUploadResult = this.generateJsonFileAndUpload(dataModelWhere, whereTemplate, jsonFileName, dataxProperties);
        } else {
            // querySql模式
            final String querySqlTemplate = dataxProperties.getOracle2Mysql().getQuerySqlTemplate();
            Map<String, Object> dataModelQuerySql = this.generateDataModelOracle2MysqlQuerySql(oracle2Mysql);
            generateJsonFileAndUploadResult = this.generateJsonFileAndUpload(dataModelQuerySql, querySqlTemplate, jsonFileName, dataxProperties);
        }
        return generateJsonFileAndUploadResult;
    }

    /**
     * 生成Oracle2MysqlQuerySql的freemarker data model
     *
     * @param oracle2Mysql Oracle2Mysql
     * @return java.util.Map<java.lang.String, java.lang.Object>
     * @author isacc 2019/5/29 11:44
     */
    private Map<String, Object> generateDataModelOracle2MysqlQuerySql(Oracle2Mysql oracle2Mysql) {
        Map<String, Object> root = new HashMap<>(16);
        // setting
        root.put(DataxParameterConstants.SETTING, oracle2Mysql.getSetting());
        // reader
        GenerateDataModelUtil.commonOracleReader(root, oracle2Mysql.getReader());
        // writer
        root.put(DataxParameterConstants.MYSQL_WRITE_MODE, oracle2Mysql.getWriter().getWriteMode());
        return GenerateDataModelUtil.commonMysqlWriter(root, oracle2Mysql.getWriter());
    }

    /**
     * 生成Oracle2MysqlWhere的freemarker data model
     *
     * @param oracle2Mysql Oracle2Mysql
     * @return java.util.Map<java.lang.String, java.lang.Object>
     * @author isacc 2019/5/29 11:44
     */
    private Map<String, Object> generateDataModelOracle2MysqlWhere(Oracle2Mysql oracle2Mysql) {
        final HashMap<String, Object> root = new HashMap<>(16);
        // 通用的
        root.put(DataxParameterConstants.SETTING, oracle2Mysql.getSetting());
        // reader
        GenerateDataModelUtil.commonOracleReader(root, oracle2Mysql.getReader());
        root.put(DataxParameterConstants.ORACLE_READER_WHERE, oracle2Mysql.getReader().getWhere());
        //  writer
        root.put(DataxParameterConstants.MYSQL_WRITE_MODE, oracle2Mysql.getWriter().getWriteMode());
        return GenerateDataModelUtil.commonMysqlWriter(root, oracle2Mysql.getWriter());
    }

}
