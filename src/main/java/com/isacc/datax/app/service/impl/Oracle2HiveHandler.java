package com.isacc.datax.app.service.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.DataxSyncDTO;
import com.isacc.datax.api.dto.Oracle2Hive;
import com.isacc.datax.app.service.DataxHandler;
import com.isacc.datax.infra.annotation.DataxHandlerType;
import com.isacc.datax.infra.config.DataxProperties;
import com.isacc.datax.infra.constant.DataxHandlerTypeConstants;
import com.isacc.datax.infra.constant.DataxParameterConstants;
import com.isacc.datax.infra.util.GenerateDataModelUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * oracle数据同步到hive
 *
 * @author isacc 2019/05/28 11:21
 */
@Service
@Slf4j
@DataxHandlerType(DataxHandlerTypeConstants.ORACLE2HIVE)
public class Oracle2HiveHandler extends BaseDataxServiceImpl implements DataxHandler {

    private final DataxProperties dataxProperties;

    public Oracle2HiveHandler(DataxProperties dataxProperties) {
        this.dataxProperties = dataxProperties;
    }

    @Override
    public ApiResult<Object> handle(DataxSyncDTO dataxSyncDTO) {
        return this.oracle2Hive(dataxSyncDTO);
    }

    private ApiResult<Object> oracle2Hive(DataxSyncDTO dataxSyncDTO) {
        final Oracle2Hive oracle2Hive = dataxSyncDTO.getOracle2Hive();
        ApiResult<Object> failureResult = ApiResult.initFailure();
        if (!Optional.ofNullable(oracle2Hive).isPresent()) {
            failureResult.setMessage("DataxSyncDTO.oracle2Hive is null!");
            return failureResult;
        }
        // 判断是where还是querySql
        ApiResult<Object> generateJsonFileAndUploadResult;
        String jsonFileName = dataxSyncDTO.getJsonFileName();
        if (Optional.ofNullable(oracle2Hive.getReader().getWhere()).isPresent()) {
            // where模式
            final String whereTemplate = dataxProperties.getOracle2Hive().getWhereTemplate();
            Map<String, Object> dataModelWhere = this.generateDataModelOracle2HiveWhere(oracle2Hive);
            generateJsonFileAndUploadResult = this.generateJsonFileAndUpload(dataModelWhere, whereTemplate, jsonFileName, dataxProperties);
        } else {
            // querySql模式
            final String querySqlTemplate = dataxProperties.getOracle2Hive().getQuerySqlTemplate();
            Map<String, Object> dataModelQuerySql = this.generateDataModelOracle2HiveQuerySql(oracle2Hive);
            generateJsonFileAndUploadResult = this.generateJsonFileAndUpload(dataModelQuerySql, querySqlTemplate, jsonFileName, dataxProperties);
        }
        return generateJsonFileAndUploadResult;
    }

    /**
     * 生成Oracle2HiveQuerySql的freemarker data model
     *
     * @param oracle2Hive Oracle2Hive
     * @return java.util.Map<java.lang.String, java.lang.Object>
     * @author isacc 2019/5/28 11:44
     */
    private Map<String, Object> generateDataModelOracle2HiveQuerySql(Oracle2Hive oracle2Hive) {
        Map<String, Object> root = new HashMap<>(16);
        // setting
        root.put(DataxParameterConstants.SETTING, oracle2Hive.getSetting());
        // reader
        GenerateDataModelUtil.commonOracleReader(root, oracle2Hive.getReader());
        // writer
        root.put(DataxParameterConstants.HDFS_WRITER_MODE, oracle2Hive.getWriter().getWriteMode());
        return GenerateDataModelUtil.commonHdfsWriter(root, oracle2Hive.getWriter());
    }

    /**
     * 生成Oracle2HiveWhere的freemarker data model
     *
     * @param oracle2Hive Oracle2Hive
     * @return java.util.Map<java.lang.String, java.lang.Object>
     * @author isacc 2019/5/28 11:44
     */
    private Map<String, Object> generateDataModelOracle2HiveWhere(Oracle2Hive oracle2Hive) {
        final HashMap<String, Object> root = new HashMap<>(16);
        // 通用的
        root.put(DataxParameterConstants.SETTING, oracle2Hive.getSetting());
        GenerateDataModelUtil.commonOracleReader(root, oracle2Hive.getReader());
        // oracle
        root.put(DataxParameterConstants.ORACLE_READER_COLUMN, oracle2Hive.getReader().getColumn());
        root.put(DataxParameterConstants.ORACLE_READER_WHERE, oracle2Hive.getReader().getWhere());
        //  hdfs
        root.put(DataxParameterConstants.HDFS_WRITER_MODE, oracle2Hive.getWriter().getWriteMode());
        return GenerateDataModelUtil.commonHdfsWriter(root, oracle2Hive.getWriter());
    }

}
