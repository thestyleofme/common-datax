package com.isacc.datax.app.service.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.DataxSyncDTO;
import com.isacc.datax.api.dto.Mysql2Mysql;
import com.isacc.datax.app.service.DataxHandler;
import com.isacc.datax.infra.annotation.DataxHandlerType;
import com.isacc.datax.infra.config.DataxProperties;
import com.isacc.datax.infra.constant.DataxHandlerTypeConstants;
import com.isacc.datax.infra.constant.DataxParameterConstants;
import com.isacc.datax.infra.util.GenerateDataModelUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * <p>
 * description
 * </P>
 *
 * @author isacc 2019/05/23 11:29
 */
@Service
@Slf4j
@DataxHandlerType(DataxHandlerTypeConstants.MYSQL2MYSQL)
public class Mysql2MysqlHandler extends BaseDataxServiceImpl implements DataxHandler {

    private final DataxProperties dataxProperties;

    public Mysql2MysqlHandler(DataxProperties dataxProperties) {
        this.dataxProperties = dataxProperties;
    }

    @Override
    public ApiResult<Object> handle(DataxSyncDTO dataxSyncDTO) {
        return this.mysql2Mysql(dataxSyncDTO);
    }

    private ApiResult<Object> mysql2Mysql(DataxSyncDTO dataxSyncDTO) {
        final Mysql2Mysql mysql2Mysql = dataxSyncDTO.getMysql2Mysql();
        ApiResult<Object> failureResult = ApiResult.initFailure();
        if (!Optional.ofNullable(mysql2Mysql).isPresent()) {
            failureResult.setMessage("DataxSyncDTO.mysql2Mysql is null!");
            return failureResult;
        }
        // 判断是where还是querySql
        ApiResult<Object> generateJsonFileAndUploadResult;
        String jsonFileName = dataxSyncDTO.getJsonFileName();
        if (Optional.ofNullable(mysql2Mysql.getReader().getWhere()).isPresent()) {
            // where模式
            final String whereTemplate = dataxProperties.getMysql2Mysql().getWhereTemplate();
            Map<String, Object> dataModelWhere = generateDataModelMysql2MysqlWhere(mysql2Mysql);
            generateJsonFileAndUploadResult = this.generateJsonFileAndUpload(dataModelWhere, whereTemplate, jsonFileName, dataxProperties);
        } else {
            // querySql模式
            final String querySqlTemplate = dataxProperties.getMysql2Mysql().getQuerySqlTemplate();
            Map<String, Object> dataModelQuery = this.generateDataModelMysql2MysqlQuery(mysql2Mysql);
            generateJsonFileAndUploadResult = this.generateJsonFileAndUpload(dataModelQuery, querySqlTemplate, jsonFileName, dataxProperties);
        }
        return generateJsonFileAndUploadResult;
    }

    /**
     * 生成mysql2mysql_querySql的freemarker data model
     *
     * @param mysql2Mysql Mysql2MysqlDTO
     * @return java.util.Map<java.lang.String, java.lang.Object>
     * @author isacc 2019/5/22 14:25
     */
    private Map<String, Object> generateDataModelMysql2MysqlQuery(Mysql2Mysql mysql2Mysql) {
        Map<String, Object> root = new HashMap<>(16);
        // setting
        root.put(DataxParameterConstants.SETTING, mysql2Mysql.getSetting());
        // reader
        GenerateDataModelUtil.commonMysqlReader(root, mysql2Mysql.getReader());
        // writer
        root.put(DataxParameterConstants.MYSQL_WRITE_MODE, mysql2Mysql.getWriter().getWriteMode());
        return GenerateDataModelUtil.commonMysqlWriter(root, mysql2Mysql.getWriter());
    }

    /**
     * 生成mysql2mysql_where的freemarker data model
     *
     * @param mysql2Mysql Mysql2MysqlDTO
     * @return java.util.Map<java.lang.String, java.lang.Object>
     * @author isacc 2019/5/22 14:25
     */
    private Map<String, Object> generateDataModelMysql2MysqlWhere(Mysql2Mysql mysql2Mysql) {
        Map<String, Object> root = generateDataModelMysql2MysqlQuery(mysql2Mysql);
        // reader
        root.put(DataxParameterConstants.MYSQL_READER_COLUMN, mysql2Mysql.getReader().getColumn());
        root.put(DataxParameterConstants.MYSQL_READER_WHERE, mysql2Mysql.getReader().getWhere());
        // writer
        root.put(DataxParameterConstants.MYSQL_WRITE_MODE, mysql2Mysql.getWriter().getWriteMode());
        return root;
    }

}
