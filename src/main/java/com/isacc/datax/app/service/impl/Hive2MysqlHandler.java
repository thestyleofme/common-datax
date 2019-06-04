package com.isacc.datax.app.service.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.DataxSyncDTO;
import com.isacc.datax.api.dto.Hive2Mysql;
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
 * Hive数据同步到MySQL处理类
 * </P>
 *
 * @author isacc 2019/05/27 17:28
 */
@Service
@Slf4j
@DataxHandlerType(DataxHandlerTypeConstants.HIVE2MYSQL)
public class Hive2MysqlHandler extends BaseDataxServiceImpl implements DataxHandler {

    private final DataxProperties dataxProperties;

    public Hive2MysqlHandler(DataxProperties dataxProperties) {
        this.dataxProperties = dataxProperties;
    }

    @Override
    public ApiResult<Object> handle(DataxSyncDTO dataxSyncDTO) {
        return this.hive2Mysql(dataxSyncDTO);
    }

    private ApiResult<Object> hive2Mysql(DataxSyncDTO dataxSyncDTO) {
        Hive2Mysql hive2Mysql = dataxSyncDTO.getHive2Mysql();
        ApiResult<Object> failureResult = ApiResult.initFailure();
        if (!Optional.ofNullable(hive2Mysql).isPresent()) {
            failureResult.setMessage("DataxSyncDTO.hive2Mysql is null!");
            return failureResult;
        }
        String jsonFileName = dataxSyncDTO.getJsonFileName();
        final String template = dataxProperties.getHive2MysqlTemplate();
        final Map<String, Object> dataModelHive2Mysql = this.generateDataModelHive2Mysql(hive2Mysql);
        final ApiResult<Object> generateJsonFileAndUploadResult = this.generateJsonFileAndUpload(dataModelHive2Mysql, template, jsonFileName, dataxProperties);
        if (!generateJsonFileAndUploadResult.getResult()) {
            return generateJsonFileAndUploadResult;
        }
        return ApiResult.initSuccess();
    }

    /**
     * @param hive2Mysql Hive2Mysql
     * @return java.util.Map<java.lang.String, java.lang.Object>
     * @author isacc 2019/5/27 20:45
     */
    private Map<String, Object> generateDataModelHive2Mysql(Hive2Mysql hive2Mysql) {
        final HashMap<String, Object> root = new HashMap<>(16);
        // 通用的
        root.put(DataxParameterConstants.SETTING, hive2Mysql.getSetting());
        // hdfsreader 参数部分
        GenerateDataModelUtil.commonHdfsReader(root, hive2Mysql.getReader());
        // mysqlwriter 参数部分
        root.put(DataxParameterConstants.MYSQL_WRITE_MODE, hive2Mysql.getWriter().getWriteMode());
        return GenerateDataModelUtil.commonMysqlWriter(root, hive2Mysql.getWriter());
    }
}
