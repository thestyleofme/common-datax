package com.isacc.datax.app.service.impl;

import java.util.*;

import javax.validation.constraints.NotBlank;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.DataxSyncDTO;
import com.isacc.datax.api.dto.Hive2Hive;
import com.isacc.datax.api.dto.HiveInfoDTO;
import com.isacc.datax.app.service.DataxHandler;
import com.isacc.datax.app.service.HiveService;
import com.isacc.datax.domain.entity.datax.HivePartition;
import com.isacc.datax.domain.entity.reader.hdfsreader.HdfsFileTypeEnum;
import com.isacc.datax.domain.repository.MysqlRepository;
import com.isacc.datax.infra.annotation.DataxHandlerType;
import com.isacc.datax.infra.config.DataxProperties;
import com.isacc.datax.infra.constant.Constants;
import com.isacc.datax.infra.constant.DataxHandlerTypeConstants;
import com.isacc.datax.infra.constant.DataxParameterConstants;
import com.isacc.datax.infra.util.DataxUtil;
import com.isacc.datax.infra.util.GenerateDataModelUtil;
import com.isacc.datax.infra.util.HdfsUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * <p>
 * description
 * </P>
 *
 * @author isacc 2019/05/23 14:46
 */
@Service
@Slf4j
@DataxHandlerType(DataxHandlerTypeConstants.HIVE2HIVE)
public class Hive2HiveHandler extends BaseDataxServiceImpl implements DataxHandler {

    private final DataxProperties dataxProperties;
    private final MysqlRepository mysqlRepository;
    private final HiveService hiveService;

    public Hive2HiveHandler(DataxProperties dataxProperties, MysqlRepository mysqlRepository, HiveService hiveService) {
        this.dataxProperties = dataxProperties;
        this.mysqlRepository = mysqlRepository;
        this.hiveService = hiveService;
    }

    @Override
    public ApiResult<Object> handle(DataxSyncDTO dataxSyncDTO) {
        return this.hive2Hive(dataxSyncDTO);
    }

    private ApiResult<Object> hive2Hive(DataxSyncDTO dataxSyncDTO) {
        final Hive2Hive hive2Hive = dataxSyncDTO.getHive2Hive();
        ApiResult<Object> failureResult = ApiResult.initFailure();
        if (!Optional.ofNullable(hive2Hive).isPresent()) {
            failureResult.setMessage("DataxSyncDTO.hive2Hive is null!");
            return failureResult;
        }
        // 判断是否是csv导入
        final ApiResult<Object> hiveDbAndTableIsExistApiResult;
        if (!HdfsFileTypeEnum.CSV.getFileType().equalsIgnoreCase(hive2Hive.getReader().getFileType())) {
            // 不是csv 校验reader中hive库/表是否存在，不存在则返回
            // 不是csv 校验writer中的库/表是否存在，不存在则创建
            hiveDbAndTableIsExistApiResult = mysqlRepository.checkHiveDbAndTable(hive2Hive);
        } else {
            // 是csv,先上传csv到hdfs
            @NotBlank String readerPath = hive2Hive.getReader().getPath();
            ApiResult<Object> uploadApiResult = HdfsUtil.upload(hive2Hive.getReader().getDefaultFS(),
                    dataxProperties.getUsername(),
                    hive2Hive.getCsvPath(),
                    readerPath.substring(0, readerPath.lastIndexOf('/')));
            if (!uploadApiResult.getResult()) {
                return uploadApiResult;
            }
            // writer进行创库创表
            hiveDbAndTableIsExistApiResult = mysqlRepository.checkWriterHiveDbAndTable(hive2Hive);
        }
        if (!hiveDbAndTableIsExistApiResult.getResult()) {
            return hiveDbAndTableIsExistApiResult;
        } else {
            // 校验 fileType fieldDelimiter writeMode
            final ApiResult<Object> checkApiResult = this.checkHdfsParams(
                    new String[]{hive2Hive.getReader().getFileType(), hive2Hive.getWriter().getFileType()},
                    hive2Hive.getReader().getFieldDelimiter(),
                    hive2Hive.getWriter().getWriteMode());
            if (!checkApiResult.getResult()) {
                return checkApiResult;
            }
            ApiResult<Object> handleHiveResult = this.handleHive(hiveDbAndTableIsExistApiResult, hive2Hive);
            if (!handleHiveResult.getResult()) {
                return handleHiveResult;
            }
        }
        // 开始导数
        final String template = dataxProperties.getHive2HiveTemplate();
        // 生成json file
        Map<String, Object> dataModel = generateDataModelHive2Hive(hive2Hive);
        return this.generateJsonFileAndUpload(dataModel, template, dataxSyncDTO.getJsonFileName(), dataxProperties);
    }

    /**
     * 根据检测hive返回的结果对hive进行处理
     *
     * @param checkResult  检测hive相关信息的返回结果
     * @param hive2Hive Hive2Hive
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019/5/9 17:43
     */
    private ApiResult<Object> handleHive(ApiResult<Object> checkResult, Hive2Hive hive2Hive) {
        final Object content = checkResult.getContent();
        if (!Objects.isNull(content)) {
            final Map contentMap = (HashMap) content;
            final Object errorType = contentMap.get("errorType");
            final Object hiveDbName = contentMap.get("hiveDbName");
            final Object hiveTblName = contentMap.get("hiveTblName");
            if (Constants.DB_IS_NOT_EXIST.equals(errorType)) {
                // 创库创表
                hiveService.createDatabase(String.valueOf(hiveDbName));
                log.info("create hive database：{}", hiveDbName);
            }
            // 判断是否分区
            @NotBlank String path = hive2Hive.getWriter().getPath();
            List<HivePartition> hivePartitionList = DataxUtil.partitionList(path);
            HiveInfoDTO hiveInfoDTO = HiveInfoDTO.builder().
                    databaseName((String) hiveDbName).
                    tableName((String) hiveTblName).
                    columns(hive2Hive.getWriter().getColumn()).
                    fieldDelimiter(hive2Hive.getWriter().getFieldDelimiter()).
                    fileType(HdfsFileTypeEnum.valueOf(hive2Hive.getWriter().getFileType().toUpperCase()).getFileType()).
                    partitionList(hivePartitionList).
                    build();
            final ApiResult<Object> createTableApiResult = hiveService.createTable(hiveInfoDTO);
            if (!createTableApiResult.getResult()) {
                return createTableApiResult;
            }
            // 若分区，还需新增分区
            if (path.contains(Constants.Symbol.EQUAL)) {
                ApiResult<Object> addPartitionApiResult = hiveService.addPartition(hiveInfoDTO);
                if (!addPartitionApiResult.getResult()) {
                    return addPartitionApiResult;
                }
            }
        }
        return ApiResult.initSuccess();
    }

    /**
     * 生成hive2hive的freemarker data model
     *
     * @param hive2Hive Hive2Hive
     * @return java.util.Map<java.lang.String, java.lang.Object>
     * @author isacc 2019/5/23 15:27
     */
    private Map<String, Object> generateDataModelHive2Hive(Hive2Hive hive2Hive) {
        final HashMap<String, Object> root = new HashMap<>(16);
        // 通用的
        root.put(DataxParameterConstants.SETTING, hive2Hive.getSetting());
        // hdfsreader 参数部分
        GenerateDataModelUtil.commonHdfsReader(root, hive2Hive.getReader());
        //  hdfswriter 参数部分
        root.put(DataxParameterConstants.HDFS_WRITER_MODE, hive2Hive.getWriter().getWriteMode());
        return GenerateDataModelUtil.commonHdfsWriter(root, hive2Hive.getWriter());
    }

}
