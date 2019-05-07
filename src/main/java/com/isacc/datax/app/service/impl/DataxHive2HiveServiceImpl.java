package com.isacc.datax.app.service.impl;

import javax.validation.constraints.NotBlank;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.Hive2HiveDTO;
import com.isacc.datax.api.dto.HiveInfoDTO;
import com.isacc.datax.app.service.DataxHive2HiveService;
import com.isacc.datax.app.service.HiveService;
import com.isacc.datax.domain.entity.reader.hdfsreader.HdfsFileTypeEnum;
import com.isacc.datax.domain.repository.MysqlRepository;
import com.isacc.datax.infra.config.DataxProperties;
import com.isacc.datax.infra.util.DataxUtil;
import com.isacc.datax.infra.util.FreemarkerUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

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

    private static final String DB_IS_NOT_EXIST = "DB_IS_NOT_EXIST";

    private final MysqlRepository mysqlRepository;
    private final DataxProperties dataxProperties;
    private final HiveService hiveService;

    @Autowired
    public DataxHive2HiveServiceImpl(MysqlRepository mysqlRepository, DataxProperties dataxProperties, HiveService hiveService) {
        this.mysqlRepository = mysqlRepository;
        this.dataxProperties = dataxProperties;
        this.hiveService = hiveService;
    }

    @SuppressWarnings("Duplicates")
    @Override
    public ApiResult<Object> hive2hive(Hive2HiveDTO hive2HiveDTO) {
        // 校验 fileType fieldDelimiter writeMode
        final ApiResult<Object> checkHdfsParamsApiResult = this.checkHdfsParams(hive2HiveDTO);
        if (!checkHdfsParamsApiResult.getResult()) {
            return checkHdfsParamsApiResult;
        }
        // 校验reader中hive库/表是否存在，不存在则返回
        // 检查writer中的库/表是否存在，不存在则创建
        final ApiResult<Object> hiveDbAndTableIsExistApiResult = mysqlRepository.hiveDbAndTableIsExist(hive2HiveDTO);
        if (!hiveDbAndTableIsExistApiResult.getResult()) {
            return hiveDbAndTableIsExistApiResult;
        } else {
            final Object content = hiveDbAndTableIsExistApiResult.getContent();
            if (!Objects.isNull(content)) {
                final Map contentMap = (HashMap) content;
                final Object errorType = contentMap.get("errorType");
                final Object hiveDbName = contentMap.get("hiveDbName");
                final Object hiveTblName = contentMap.get("hiveTblName");
                if (DB_IS_NOT_EXIST.equals(errorType)) {
                    // 创库创表
                    hiveService.createDatabase(String.valueOf(hiveDbName));
                    log.info("create hive database：{}", hiveDbName);
                }
                final ApiResult<Object> createTableApiResult = hiveService.createTable(HiveInfoDTO.builder().
                        databaseName((String) hiveDbName).
                        tableName((String) hiveTblName).
                        columns(hive2HiveDTO.getWriter().getColumn())
                        .fieldDelimiter(hive2HiveDTO.getWriter().getFieldDelimiter())
                        .fileType(HdfsFileTypeEnum.valueOf(hive2HiveDTO.getWriter().getFileType().toUpperCase()).getFileType())
                        .build());
                if (!createTableApiResult.getResult()) {
                    return createTableApiResult;
                }
            }
        }
        // 使用freemarker生成json文件
        final String noDtTemplate = dataxProperties.getHive2Hive().getNoDtTemplate();
        final ApiResult<Object> createJsonApiResult = FreemarkerUtil.createJsonFile(generateDataModel(hive2HiveDTO), dataxProperties, noDtTemplate);
        if (!createJsonApiResult.getResult()) {
            return createJsonApiResult;
        }
        final File jsonFile = (File) createJsonApiResult.getContent();
        // file转为MultipartFile
        final ApiResult<Object> file2MultiApiResult = this.file2MultipartFile(jsonFile);
        if (!file2MultiApiResult.getResult()) {
            return file2MultiApiResult;
        }
        final MultipartFile multipartFile = (MultipartFile) file2MultipartFile(jsonFile).getContent();
        // 上传到datax服务器
        final ApiResult<Object> uploadFileApiResult = this.uploadFile(multipartFile, dataxProperties);
        if (!uploadFileApiResult.getResult()) {
            return uploadFileApiResult;
        }
        // 远程执行python进行导数
        return execCommand(dataxProperties, String.valueOf(uploadFileApiResult.getContent()));
    }

    /**
     * 检验hdfsreader以及hdfswriter的fileType fieldDelimiter writeMode
     *
     * @param hive2HiveDTO Hive2HiveDTO
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019-05-07 20:58
     */
    @SuppressWarnings("Duplicates")
    private ApiResult<Object> checkHdfsParams(Hive2HiveDTO hive2HiveDTO) {
        // fileType
        @NotBlank final String readerFileType = hive2HiveDTO.getReader().getFileType();
        @NotBlank final String writerFileType = hive2HiveDTO.getWriter().getFileType();
        final ApiResult<Object> checkReaderHdfsFileTypeApiResult = DataxUtil.checkHdfsFileType(readerFileType, writerFileType);
        if (!checkReaderHdfsFileTypeApiResult.getResult()) {
            return checkReaderHdfsFileTypeApiResult;
        }
        // fieldDelimiter
        final String readerFieldDelimiter = hive2HiveDTO.getReader().getFieldDelimiter();
        @NotBlank final String writerFieldDelimiter = hive2HiveDTO.getWriter().getFieldDelimiter();
        final ApiResult<Object> checkReaderFieldDelimiterApiResult = DataxUtil.checkFieldDelimiter(readerFieldDelimiter, writerFieldDelimiter);
        if (!checkReaderFieldDelimiterApiResult.getResult()) {
            return checkReaderFieldDelimiterApiResult;
        }
        // writeMode
        final ApiResult<Object> writeModeApiResult = DataxUtil.checkWriteMode(hive2HiveDTO.getWriter().getWriteMode());
        if (!writeModeApiResult.getResult()) {
            return writeModeApiResult;
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
