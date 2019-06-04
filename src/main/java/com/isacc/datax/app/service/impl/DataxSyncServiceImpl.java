package com.isacc.datax.app.service.impl;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.DataxSyncDTO;
import com.isacc.datax.app.service.AzkabanService;
import com.isacc.datax.app.service.DataxHandler;
import com.isacc.datax.app.service.DataxSyncService;
import com.isacc.datax.infra.config.AzkabanProperties;
import com.isacc.datax.infra.config.DataxHandlerContext;
import com.isacc.datax.infra.config.DataxProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 数据同步表应用服务默认实现
 *
 * @author isacc 2019-05-17 14:07:48
 */
@Service
@Slf4j
public class DataxSyncServiceImpl extends BaseDataxServiceImpl implements DataxSyncService {

    private final DataxProperties dataxProperties;
    private final AzkabanProperties azkabanProperties;
    private final AzkabanService azkabanService;
    private final DataxHandlerContext dataxHandlerContext;

    public DataxSyncServiceImpl(DataxProperties dataxProperties, AzkabanProperties azkabanProperties, AzkabanService azkabanService, DataxHandlerContext dataxHandlerContext) {
        this.dataxProperties = dataxProperties;
        this.azkabanProperties = azkabanProperties;
        this.azkabanService = azkabanService;
        this.dataxHandlerContext = dataxHandlerContext;
    }

    @Override
    @Transactional(rollbackFor = {Exception.class})
    public ApiResult<Object> execute(DataxSyncDTO dataxSyncDTO) {
        final ApiResult<Object> failureResult = ApiResult.initFailure();
        final String sourceDatasourceType = dataxSyncDTO.getSourceDatasourceType();
        final String writeDatasourceType = dataxSyncDTO.getWriteDatasourceType();
        if (StringUtils.isBlank(sourceDatasourceType) || StringUtils.isBlank(writeDatasourceType)) {
            failureResult.setMessage("sourceDatasourceType and writeDatasourceType required not null!");
            return failureResult;
        }
        String type = String.format("%s-%s", sourceDatasourceType, writeDatasourceType).toUpperCase();
        DataxHandler handler;
        try {
            handler = dataxHandlerContext.getInstance(type);
        } catch (Exception e) {
            log.error("can't find {} handler class!", type);
            failureResult.setMessage(String.format("can't find %s handler class! please add @DataxHandlerType annotation to the handler class!", type));
            return failureResult;
        }
        // 生成datax json file以及上传到datax服务器
        ApiResult<Object> generateJsonFileAndUploadResult = handler.handle(dataxSyncDTO);
        if (!generateJsonFileAndUploadResult.getResult()) {
            return generateJsonFileAndUploadResult;
        }
        // azkaban调度进行执行python
        return this.azkabanImmediateExecution(dataxSyncDTO);
    }

    private ApiResult<Object> azkabanImmediateExecution(DataxSyncDTO dataxSyncDTO) {
        ApiResult<Object> successResult = ApiResult.initSuccess();
        // 目前target都事先创建好库/表、分区
        // 生成模板json file 上传到datax服务器
        // 这里先一次性执行，使用azkaban调度运行
        final String jsonFileName = dataxSyncDTO.getJsonFileName();
        ApiResult<Object> generateAzkabanZipResult = this.generateAzkabanZip(jsonFileName, azkabanProperties, dataxProperties);
        if (!generateAzkabanZipResult.getResult()) {
            return generateAzkabanZipResult;
        }
        String zipName = jsonFileName.substring(0, jsonFileName.indexOf('.'));
        String zipPath = azkabanProperties.getLocalDicPath() + zipName + ".zip";
        ApiResult<Object> executeResult = azkabanService.executeDataxJob(zipName, zipName, zipPath);
        if (!executeResult.getResult()) {
            return executeResult;
        }
        // 回写datax_sync表插入本次同步信息
        ApiResult<Object> writeDataxSettingInfoResult = this.writeDataxSettingInfo(dataxSyncDTO, dataxProperties, azkabanProperties);
        if (!writeDataxSettingInfoResult.getResult()) {
            return writeDataxSettingInfoResult;
        }
        // todo
        successResult.setMessage("execute datax job successfully!");
        successResult.setContent(executeResult.getContent());
        return successResult;
    }

    @Override
    public String generateDataxCommand() {
        return String.format("python %sbin/datax.py %s%%s", dataxProperties.getHome(), dataxProperties.getUploadDicPath());
    }

}
