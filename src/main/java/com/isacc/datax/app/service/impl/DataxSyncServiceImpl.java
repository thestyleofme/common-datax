package com.isacc.datax.app.service.impl;

import java.util.Optional;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.DataxSyncDTO;
import com.isacc.datax.app.service.AzkabanService;
import com.isacc.datax.app.service.DataxHandler;
import com.isacc.datax.app.service.DataxSyncService;
import com.isacc.datax.domain.entity.DataxSync;
import com.isacc.datax.domain.repository.DataxSyncRepository;
import com.isacc.datax.infra.config.AzkabanProperties;
import com.isacc.datax.infra.config.DataxHandlerContext;
import com.isacc.datax.infra.config.DataxProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Service;

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
    private final DataxSyncRepository dataxSyncRepository;

    public DataxSyncServiceImpl(DataxProperties dataxProperties, AzkabanProperties azkabanProperties, AzkabanService azkabanService, DataxHandlerContext dataxHandlerContext, DataxSyncRepository dataxSyncRepository) {
        this.dataxProperties = dataxProperties;
        this.azkabanProperties = azkabanProperties;
        this.azkabanService = azkabanService;
        this.dataxHandlerContext = dataxHandlerContext;
        this.dataxSyncRepository = dataxSyncRepository;
    }

    /**
     *<p>这里不加事务， 因为需要在一个方法里切换多个数据源，会出问题</p>
     *<a>https://gitee.com/baomidou/dynamic-datasource-spring-boot-starter/wikis/pages?sort_id=1030627&doc_id=147063</a>
     */
    @Override
    public ApiResult<Object> execute(DataxSyncDTO dataxSyncDTO) {
        final ApiResult<Object> failureResult = ApiResult.initFailure();
        final String sourceDatasourceType = dataxSyncDTO.getSourceDatasourceType();
        final String writeDatasourceType = dataxSyncDTO.getWriteDatasourceType();
        if (StringUtils.isBlank(sourceDatasourceType) || StringUtils.isBlank(writeDatasourceType)) {
            failureResult.setMessage("sourceDatasourceType and writeDatasourceType required not null!");
            return failureResult;
        }
        // todo 数据源表 这里根据数据源ID去查找username password等信息 后续优化
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
        if (!Optional.ofNullable(dataxSyncDTO.getSyncId()).isPresent()) {
            dataxSyncRepository.insertSelectiveDTO(dataxSyncDTO);
        } else {
            dataxSyncRepository.updateSelectiveDTO(dataxSyncDTO);
        }
        successResult.setMessage("execute datax job successfully!");
        successResult.setContent(executeResult.getContent());
        return successResult;
    }

    @Override
    public String generateDataxCommand() {
        return String.format("python %sbin/datax.py %s%%s", dataxProperties.getHome(), dataxProperties.getUploadDicPath());
    }

    @Override
    public ApiResult<Object> deleteDataxSync(DataxSyncDTO dataxSyncDTO) {
        ApiResult<Object> successResult = ApiResult.initSuccess();
        // 先删除原有的json
        ApiResult<Object> deleteDataxJsonFileResult = this.deleteDataxJsonFile(dataxProperties, dataxSyncRepository.selectByPrimaryKey(dataxSyncDTO.getSyncId()));
        if (!deleteDataxJsonFileResult.getResult()) {
            return deleteDataxJsonFileResult;
        }
        // 删除表数据
        dataxSyncRepository.deleteByPrimaryKey(dataxSyncDTO.getSyncId());
        return successResult;
    }

    @Override
    public ApiResult<Object> checkSyncNameAndJsonFileName(DataxSyncDTO dataxSyncDTO) {
        final ApiResult<Object> failResult = ApiResult.initFailure();
        final int syncNameCount = dataxSyncRepository.selectCount(DataxSync.builder().syncName(dataxSyncDTO.getSyncName()).build());
        if (syncNameCount > 0) {
            failResult.setMessage("the datax sync name already exists!");
            return failResult;
        }
        final int jsonFileNameCount = dataxSyncRepository.selectCount(DataxSync.builder().jsonFileName(dataxSyncDTO.getJsonFileName()).build());
        if (jsonFileNameCount > 0) {
            failResult.setMessage("the datax json file name already exists!");
            return failResult;
        }
        return ApiResult.initSuccess();
    }

}
