package com.isacc.datax.app.service.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.DataxSyncDTO;
import com.isacc.datax.api.dto.Mysql2Oracle;
import com.isacc.datax.app.service.DataxHandler;
import com.isacc.datax.infra.annotation.DataxHandlerType;
import com.isacc.datax.infra.config.DataxProperties;
import com.isacc.datax.infra.constant.DataxHandlerTypeConstants;
import com.isacc.datax.infra.constant.DataxParameterConstants;
import com.isacc.datax.infra.util.GenerateDataModelUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * mysql数据同步到oracle
 *
 * @author isacc 2019/05/29 15:04
 */
@Service
@Slf4j
@DataxHandlerType(DataxHandlerTypeConstants.MYSQL2ORACLE)
public class Mysql2OracleHandler extends BaseDataxServiceImpl implements DataxHandler {

    private final DataxProperties dataxProperties;

    public Mysql2OracleHandler(DataxProperties dataxProperties) {
        this.dataxProperties = dataxProperties;
    }

    @Override
    public ApiResult<Object> handle(DataxSyncDTO dataxSyncDTO) {
        return this.mysql2Oracle(dataxSyncDTO);
    }

    private ApiResult<Object> mysql2Oracle(DataxSyncDTO dataxSyncDTO){
        final Mysql2Oracle mysql2Oracle = dataxSyncDTO.getMysql2Oracle();
        ApiResult<Object> failureResult = ApiResult.initFailure();
        if (!Optional.ofNullable(mysql2Oracle).isPresent()) {
            failureResult.setMessage("DataxSyncDTO.mysql2Oracle is null!");
            return failureResult;
        }
        // 判断是where还是querySql
        ApiResult<Object> generateJsonFileAndUploadResult;
        String jsonFileName = dataxSyncDTO.getJsonFileName();
        if (Optional.ofNullable(mysql2Oracle.getReader().getWhere()).isPresent()) {
            // where模式
            final String whereTemplate = dataxProperties.getMysql2Oracle().getWhereTemplate();
            Map<String, Object> dataModelWhere = generateDataModelMysql2OracleWhere(mysql2Oracle);
            generateJsonFileAndUploadResult = this.generateJsonFileAndUpload(dataModelWhere, whereTemplate, jsonFileName, dataxProperties);
        } else {
            // querySql模式
            final String querySqlTemplate = dataxProperties.getMysql2Oracle().getQuerySqlTemplate();
            Map<String, Object> dataModelQuery = this.generateDataModelMysql2OracleQuery(mysql2Oracle);
            generateJsonFileAndUploadResult = this.generateJsonFileAndUpload(dataModelQuery, querySqlTemplate, jsonFileName, dataxProperties);
        }
        return generateJsonFileAndUploadResult;
    }

    private Map<String, Object> generateDataModelMysql2OracleQuery(Mysql2Oracle mysql2Oracle) {
        Map<String, Object> root = new HashMap<>(16);
        // setting
        root.put(DataxParameterConstants.SETTING, mysql2Oracle.getSetting());
        // reader
        GenerateDataModelUtil.commonMysqlReader(root, mysql2Oracle.getReader());
        // writer
        return GenerateDataModelUtil.commonOracleWriter(root, mysql2Oracle.getWriter());
    }

    private Map<String, Object> generateDataModelMysql2OracleWhere(Mysql2Oracle mysql2Oracle) {
        Map<String, Object> root = generateDataModelMysql2OracleQuery(mysql2Oracle);
        // reader
        root.put(DataxParameterConstants.MYSQL_READER_COLUMN, mysql2Oracle.getReader().getColumn());
        root.put(DataxParameterConstants.MYSQL_READER_WHERE, mysql2Oracle.getReader().getWhere());
        // writer
        return root;
    }


}
