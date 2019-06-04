package com.isacc.datax.app.service.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.DataxSyncDTO;
import com.isacc.datax.api.dto.Oracle2Oracle;
import com.isacc.datax.app.service.DataxHandler;
import com.isacc.datax.infra.annotation.DataxHandlerType;
import com.isacc.datax.infra.config.DataxProperties;
import com.isacc.datax.infra.constant.DataxHandlerTypeConstants;
import com.isacc.datax.infra.constant.DataxParameterConstants;
import com.isacc.datax.infra.util.GenerateDataModelUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * oracle数据同步到oracle
 *
 * @author isacc 2019/05/29 11:21
 */
@Service
@Slf4j
@DataxHandlerType(DataxHandlerTypeConstants.ORACLE2ORACLE)
public class Oracle2OracleHandler extends BaseDataxServiceImpl implements DataxHandler {

    private final DataxProperties dataxProperties;

    public Oracle2OracleHandler(DataxProperties dataxProperties) {
        this.dataxProperties = dataxProperties;
    }

    @Override
    public ApiResult<Object> handle(DataxSyncDTO dataxSyncDTO) {
        return this.oracle2Oracle(dataxSyncDTO);
    }

    private ApiResult<Object> oracle2Oracle(DataxSyncDTO dataxSyncDTO) {
        final Oracle2Oracle oracle2Oracle = dataxSyncDTO.getOracle2Oracle();
        ApiResult<Object> failureResult = ApiResult.initFailure();
        if (!Optional.ofNullable(oracle2Oracle).isPresent()) {
            failureResult.setMessage("DataxSyncDTO.oracle2Oracle is null!");
            return failureResult;
        }
        // 判断是where还是querySql
        ApiResult<Object> generateJsonFileAndUploadResult;
        String jsonFileName = dataxSyncDTO.getJsonFileName();
        if (Optional.ofNullable(oracle2Oracle.getReader().getWhere()).isPresent()) {
            // where模式
            final String whereTemplate = dataxProperties.getOracle2Oracle().getWhereTemplate();
            Map<String, Object> dataModelWhere = this.generateDataModelOracle2OracleWhere(oracle2Oracle);
            generateJsonFileAndUploadResult = this.generateJsonFileAndUpload(dataModelWhere, whereTemplate, jsonFileName, dataxProperties);
        } else {
            // querySql模式
            final String querySqlTemplate = dataxProperties.getOracle2Oracle().getQuerySqlTemplate();
            Map<String, Object> dataModelQuerySql = this.generateDataModelOracle2OracleQuerySql(oracle2Oracle);
            generateJsonFileAndUploadResult = this.generateJsonFileAndUpload(dataModelQuerySql, querySqlTemplate, jsonFileName, dataxProperties);
        }
        return generateJsonFileAndUploadResult;
    }

    /**
     * 生成Oracle2OracleQuerySql的freemarker data model
     *
     * @param oracle2Oracle Oracle2Oracles
     * @return java.util.Map<java.lang.String, java.lang.Object>
     * @author isacc 2019/5/29 11:44
     */
    private Map<String, Object> generateDataModelOracle2OracleQuerySql(Oracle2Oracle oracle2Oracle) {
        Map<String, Object> root = new HashMap<>(16);
        // setting
        root.put(DataxParameterConstants.SETTING, oracle2Oracle.getSetting());
        // reader
        GenerateDataModelUtil.commonOracleReader(root, oracle2Oracle.getReader());
        // writer
        return GenerateDataModelUtil.commonOracleWriter(root, oracle2Oracle.getWriter());
    }

    /**
     * 生成Oracle2OracleWhere的freemarker data model
     *
     * @param oracle2Oracle Oracle2Oracle
     * @return java.util.Map<java.lang.String, java.lang.Object>
     * @author isacc 2019/5/29 11:44
     */
    private Map<String, Object> generateDataModelOracle2OracleWhere(Oracle2Oracle oracle2Oracle) {
        final HashMap<String, Object> root = new HashMap<>(16);
        // 通用的
        root.put(DataxParameterConstants.SETTING, oracle2Oracle.getSetting());
        // reader
        GenerateDataModelUtil.commonOracleReader(root, oracle2Oracle.getReader());
        root.put(DataxParameterConstants.ORACLE_READER_WHERE, oracle2Oracle.getReader().getWhere());
        //  writer
        return GenerateDataModelUtil.commonOracleWriter(root, oracle2Oracle.getWriter());
    }

}
