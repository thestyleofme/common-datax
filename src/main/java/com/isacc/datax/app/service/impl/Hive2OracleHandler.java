package com.isacc.datax.app.service.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.DataxSyncDTO;
import com.isacc.datax.api.dto.Hive2Oracle;
import com.isacc.datax.app.service.DataxHandler;
import com.isacc.datax.infra.annotation.DataxHandlerType;
import com.isacc.datax.infra.config.DataxProperties;
import com.isacc.datax.infra.constant.DataxHandlerTypeConstants;
import com.isacc.datax.infra.constant.DataxParameterConstants;
import com.isacc.datax.infra.util.GenerateDataModelUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * hive数据同步到oracle处理类
 *
 * @author isacc 2019/05/29 14:34
 */
@Service
@Slf4j
@DataxHandlerType(DataxHandlerTypeConstants.HIVE2ORACLE)
public class Hive2OracleHandler extends BaseDataxServiceImpl implements DataxHandler {

    private final DataxProperties dataxProperties;

    public Hive2OracleHandler(DataxProperties dataxProperties) {
        this.dataxProperties = dataxProperties;
    }

    @Override
    public ApiResult<Object> handle(DataxSyncDTO dataxSyncDTO) {
        return this.hive2Oracle(dataxSyncDTO);
    }

    private ApiResult<Object> hive2Oracle(DataxSyncDTO dataxSyncDTO) {
        final Hive2Oracle hive2Oracle = dataxSyncDTO.getHive2Oracle();
        ApiResult<Object> failureResult = ApiResult.initFailure();
        if (!Optional.ofNullable(hive2Oracle).isPresent()) {
            failureResult.setMessage("DataxSyncDTO.hive2Oracle is null!");
            return failureResult;
        }
        String jsonFileName = dataxSyncDTO.getJsonFileName();
        final String template = dataxProperties.getHive2OracleTemplate();
        final Map<String, Object> dataModelHive2Mysql = this.generateDataModelHive2Oracle(hive2Oracle);
        final ApiResult<Object> generateJsonFileAndUploadResult = this.generateJsonFileAndUpload(dataModelHive2Mysql, template, jsonFileName, dataxProperties);
        if (!generateJsonFileAndUploadResult.getResult()) {
            return generateJsonFileAndUploadResult;
        }
        return ApiResult.initSuccess();
    }

    private Map<String, Object> generateDataModelHive2Oracle(Hive2Oracle hive2Oracle) {
        final HashMap<String, Object> root = new HashMap<>(16);
        // 通用的
        root.put(DataxParameterConstants.SETTING, hive2Oracle.getSetting());
        // reader
        GenerateDataModelUtil.commonHdfsReader(root, hive2Oracle.getReader());
        // writer
        return GenerateDataModelUtil.commonOracleWriter(root, hive2Oracle.getWriter());
    }

}
