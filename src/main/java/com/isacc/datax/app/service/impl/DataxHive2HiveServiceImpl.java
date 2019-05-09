package com.isacc.datax.app.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.validation.constraints.NotBlank;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.Hive2HiveDTO;
import com.isacc.datax.api.dto.HiveInfoDTO;
import com.isacc.datax.app.service.DataxHive2HiveService;
import com.isacc.datax.app.service.HiveService;
import com.isacc.datax.domain.entity.datax.HivePartition;
import com.isacc.datax.domain.entity.reader.hdfsreader.HdfsFileTypeEnum;
import com.isacc.datax.domain.repository.MysqlRepository;
import com.isacc.datax.infra.config.DataxProperties;
import com.isacc.datax.infra.constant.Constants;
import com.isacc.datax.infra.util.DataxUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * <p>
 * DataX Hive2Hive Service Impl
 * </p>
 *
 * @author isacc 2019/05/07 14:19
 */
@Service
@Slf4j
public class DataxHive2HiveServiceImpl extends BaseServiceImpl implements DataxHive2HiveService {

    private final MysqlRepository mysqlRepository;
    private final DataxProperties dataxProperties;
    private final HiveService hiveService;

    @Autowired
    public DataxHive2HiveServiceImpl(MysqlRepository mysqlRepository, DataxProperties dataxProperties, HiveService hiveService) {
        this.mysqlRepository = mysqlRepository;
        this.dataxProperties = dataxProperties;
        this.hiveService = hiveService;
    }


    @Override
    public ApiResult<Object> hive2hive(Hive2HiveDTO hive2HiveDTO) {
        // 校验reader中hive库/表是否存在，不存在则返回
        // 校验writer中的库/表是否存在，不存在则创建
        final ApiResult<Object> hiveDbAndTableIsExistApiResult = mysqlRepository.hiveDbAndTableIsExist(hive2HiveDTO);
        if (!hiveDbAndTableIsExistApiResult.getResult()) {
            return hiveDbAndTableIsExistApiResult;
        } else {
            // 校验 fileType fieldDelimiter writeMode
            final ApiResult<Object> checkApiResult = this.checkHdfsParams(
                    new String[]{hive2HiveDTO.getReader().getFileType(), hive2HiveDTO.getWriter().getFileType()},
                    hive2HiveDTO.getReader().getFieldDelimiter(),
                    hive2HiveDTO.getWriter().getWriteMode());
            if (!checkApiResult.getResult()) {
                return checkApiResult;
            }
            ApiResult<Object> handleHiveResult = this.handleHive(hiveDbAndTableIsExistApiResult, hive2HiveDTO);
            if (!handleHiveResult.getResult()) {
                return handleHiveResult;
            }
        }
        // 开始导数
        final String template = dataxProperties.getHive2Hive().getTemplate();
        final Map<String, Object> dataModel = generateDataModel(hive2HiveDTO);
        return this.afterCheckOperations(dataModel, dataxProperties, template);
    }

    /**
     * 根据检测hive返回的结果对hive进行处理
     *
     * @param checkResult  检测hive相关信息的返回结果
     * @param hive2HiveDTO Hive2HiveDTO
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019/5/9 17:43
     */
    private ApiResult<Object> handleHive(ApiResult<Object> checkResult, Hive2HiveDTO hive2HiveDTO) {
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
            @NotBlank String path = hive2HiveDTO.getWriter().getPath();
            List<HivePartition> hivePartitionList = DataxUtil.partitionList(path);
            HiveInfoDTO hiveInfoDTO = HiveInfoDTO.builder().
                    databaseName((String) hiveDbName).
                    tableName((String) hiveTblName).
                    columns(hive2HiveDTO.getWriter().getColumn()).
                    fieldDelimiter(hive2HiveDTO.getWriter().getFieldDelimiter()).
                    fileType(HdfsFileTypeEnum.valueOf(hive2HiveDTO.getWriter().getFileType().toUpperCase()).getFileType()).
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
     * 生成freemarker的dataModel
     *
     * @param hive2HiveDTO Hive2HiveDTO
     * @return java.util.Map<java.lang.String, java.lang.Object>
     * @author isacc 2019-05-07 21:00
     */
    private Map<String, Object> generateDataModel(Hive2HiveDTO hive2HiveDTO) {
        final HashMap<String, Object> root = new HashMap<>(16);
        // setting
        root.put("setting", hive2HiveDTO.getSetting());
        // reader
        root.put("readerPath", hive2HiveDTO.getReader().getPath());
        root.put("readerDefaultFS", hive2HiveDTO.getReader().getDefaultFS());
        root.put("readerColumns", hive2HiveDTO.getReader().getColumn());
        root.put("readerType", hive2HiveDTO.getReader().getFileType());
        root.put("fieldDelimiter", hive2HiveDTO.getReader().getFieldDelimiter());
        // writer
        root.put("writerDefaultFS", hive2HiveDTO.getWriter().getDefaultFS());
        root.put("writerType", hive2HiveDTO.getWriter().getFileType());
        root.put("writerPath", hive2HiveDTO.getWriter().getPath());
        root.put("fileName", hive2HiveDTO.getWriter().getFileName());
        root.put("writerColumns", hive2HiveDTO.getWriter().getColumn());
        root.put("writeMode", hive2HiveDTO.getWriter().getWriteMode());
        return root;
    }

}
