package com.isacc.datax.app.service.impl;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

import javax.validation.constraints.NotBlank;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.HiveInfoDTO;
import com.isacc.datax.api.dto.Mysql2HiveDTO;
import com.isacc.datax.app.service.DataxMysql2HiveService;
import com.isacc.datax.app.service.HiveService;
import com.isacc.datax.domain.entity.reader.hdfsreader.HdfsFileTypeEnum;
import com.isacc.datax.domain.entity.reader.mysqlreader.MysqlReaderConnection;
import com.isacc.datax.infra.config.DataxProperties;
import com.isacc.datax.infra.mapper.MysqlSimpleMapper;
import com.isacc.datax.infra.util.DataxUtil;
import com.isacc.datax.infra.util.FreemarkerUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

/**
 * <p>
 * DataX Service Impl
 * </p>
 *
 * @author isacc 2019/04/29 17:06
 */
@SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
@Service
@Slf4j
public class DataxMysql2HiveServiceImpl extends BaseServiceImpl implements DataxMysql2HiveService {

    private final MysqlSimpleMapper mysqlSimpleMapper;
    private final HiveService hiveService;
    private final DataxProperties dataxProperties;

    @Autowired
    public DataxMysql2HiveServiceImpl(MysqlSimpleMapper mysqlSimpleMapper, HiveService hiveService, DataxProperties dataxProperties) {
        this.mysqlSimpleMapper = mysqlSimpleMapper;
        this.hiveService = hiveService;
        this.dataxProperties = dataxProperties;
    }

    @SuppressWarnings("Duplicates")
    @Override
    public ApiResult<Object> mysql2HiveWhere(Mysql2HiveDTO mysql2HiveDTO) {
        // 判断mysql的数据库/表是否存在，无则报错返回
        final ApiResult<Object> mysqlApiResult = this.mysqlDbAndTblIsExist(mysql2HiveDTO);
        if (!mysqlApiResult.getResult()) {
            return mysqlApiResult;
        }
        // 检验fileType fieldDelimiter(单字符)
        if (!this.checkHiveTableInfo(mysql2HiveDTO).getResult()) {
            return this.checkHiveTableInfo(mysql2HiveDTO);
        }
        // 判断hive的数据库/表是否存在，无则创建库表
        final ApiResult<Object> hiveApiResult = this.hiveDbAndTblIsExist(mysql2HiveDTO);
        if (!hiveApiResult.getResult()) {
            return hiveApiResult;
        }
        // 校验writeMode
        final ApiResult<Object> writeModeApiResult = DataxUtil.checkWriteMode(mysql2HiveDTO.getWriter().getWriteMode());
        if (!writeModeApiResult.getResult()) {
            return writeModeApiResult;
        }
        // 创建datax job json文件
        final String whereTemplate = dataxProperties.getMysql2Hive().getWhereTemplate();
        final ApiResult<Object> jsonResult = FreemarkerUtil.createJsonFile(generateDataModel(mysql2HiveDTO), dataxProperties, whereTemplate);
        if (!jsonResult.getResult()) {
            return jsonResult;
        }
        final File jsonFile = (File) jsonResult.getContent();
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
     * 创建freemarker的DataModel
     *
     * @param mysql2HiveDTO Mysql2HiveDTO
     * @return java.util.Map<java.lang.String, java.lang.Object>
     * @author isacc 2019-05-05 16:04
     */
    private Map<String, Object> generateDataModel(Mysql2HiveDTO mysql2HiveDTO) {
        final HashMap<String, Object> root = new HashMap<>(16);
        // setting
        root.put("setting", mysql2HiveDTO.getSetting());
        // mysql
        root.put("username", mysql2HiveDTO.getReader().getUsername());
        root.put("password", mysql2HiveDTO.getReader().getPassword());
        root.put("mysqlColumn", mysql2HiveDTO.getReader().getColumn());
        root.put("connection", mysql2HiveDTO.getReader().getConnection());
        root.put("where", mysql2HiveDTO.getReader().getWhere());
        // hdfs
        root.put("hdfsColumn", mysql2HiveDTO.getWriter().getColumn());
        root.put("defaultFS", mysql2HiveDTO.getWriter().getDefaultFS());
        root.put("fileType", mysql2HiveDTO.getWriter().getFileType());
        root.put("path", mysql2HiveDTO.getWriter().getPath());
        root.put("fileName", mysql2HiveDTO.getWriter().getFileName());
        root.put("writeMode", mysql2HiveDTO.getWriter().getWriteMode());
        root.put("fieldDelimiter", mysql2HiveDTO.getWriter().getFieldDelimiter());
        return root;
    }

    /**
     * 创建hive数据表
     *
     * @param mysql2HiveDTO Mysql2HiveDTO
     * @param hiveDb        hive数据库
     * @param hiveTable     hive表
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019-05-02 4:31
     */
    private ApiResult<Object> createHiveTable(Mysql2HiveDTO mysql2HiveDTO, String hiveDb, String hiveTable) {
        try {
            hiveService.createTable(HiveInfoDTO.builder().
                    databaseName(hiveDb).
                    tableName(hiveTable).
                    columns(mysql2HiveDTO.getWriter().getColumn())
                    .fieldDelimiter(mysql2HiveDTO.getWriter().getFieldDelimiter())
                    .fileType(HdfsFileTypeEnum.valueOf(mysql2HiveDTO.getWriter().getFileType().toUpperCase()).getFileType())
                    .build());
            log.info("create hive table:{}.{}", hiveDb, hiveTable);
            return ApiResult.initSuccess();
        } catch (Exception e) {
            final ApiResult<Object> failureApiResult = ApiResult.initFailure();
            failureApiResult.setMessage("there are something went wrong in hive!");
            failureApiResult.setContent("error: " + e.getMessage());
            return failureApiResult;
        }
    }

    /**
     * 检验fileType fieldDelimiter(单字符)
     *
     * @param mysql2HiveDTO Mysql2HiveDTO
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019-05-02 3:44
     */
    private ApiResult<Object> checkHiveTableInfo(Mysql2HiveDTO mysql2HiveDTO) {
        @NotBlank String fileType = mysql2HiveDTO.getWriter().getFileType();
        final ApiResult<Object> checkHdfsFileTypeApiResult = DataxUtil.checkHdfsFileType(fileType);
        if (!checkHdfsFileTypeApiResult.getResult()) {
            return checkHdfsFileTypeApiResult;
        }
        @NotBlank String fieldDelimiter = mysql2HiveDTO.getWriter().getFieldDelimiter();
        final ApiResult<Object> checkFieldDelimiterApiResult = DataxUtil.checkFieldDelimiter(fieldDelimiter);
        if (!checkFieldDelimiterApiResult.getResult()) {
            return checkFieldDelimiterApiResult;
        }
        return ApiResult.initSuccess();
    }

    /**
     * 校验hive的数据库、表是否存在
     *
     * @param mysql2HiveDTO Mysql2HiveDTO
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019-05-02 2:25
     */
    private ApiResult<Object> hiveDbAndTblIsExist(Mysql2HiveDTO mysql2HiveDTO) {
        @NotBlank String path = mysql2HiveDTO.getWriter().getPath();
        String hivePath = path.substring(0, path.lastIndexOf('/'));
        String hiveDbName = hivePath.substring(hivePath.lastIndexOf('/') + 1, hivePath.indexOf('.'));
        String hiveTblName = path.substring(path.lastIndexOf('/') + 1, path.lastIndexOf('?') == -1 ? path.length() : path.lastIndexOf('?'));
        final Map<String, Object> hiveDbInfoMap = mysqlSimpleMapper.hiveDbIsExist(hiveDbName);
        if (Objects.isNull(hiveDbInfoMap)) {
            // 不存在hive数据库，先创建库，再根据所选字段创建表
            final ApiResult<Object> createDbApiResult = hiveService.createDatabase(hiveDbName);
            if (!createDbApiResult.getResult()) {
                return createDbApiResult;
            }
        }
        // 存在hive数据库但不存在表，根据所选字段创建表
        return this.createHiveTable(mysql2HiveDTO, hiveDbName, hiveTblName);
    }

    /**
     * 校验mysql的数据库、表是否存在
     *
     * @param mysql2HiveDTO Mysql2HiveDTO
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019-04-29 21:11
     */
    private ApiResult<Object> mysqlDbAndTblIsExist(Mysql2HiveDTO mysql2HiveDTO) {
        final ApiResult<Object> failureApiResult = ApiResult.initFailure();
        final List<String> databaseNameList = new ArrayList<>(5);
        final List<String> tableList = new ArrayList<>(10);
        mysql2HiveDTO.getReader().getConnection().stream().map(MysqlReaderConnection::getJdbcUrl).forEach(jdbcUrls -> jdbcUrls.forEach(url ->
                databaseNameList.add(url.substring(url.lastIndexOf('/') + 1, !url.contains("?") ? url.length() : url.indexOf('?')))
        ));
        List<String> collect = databaseNameList.stream().distinct().collect(Collectors.toList());
        if (collect.size() != 1) {
            failureApiResult.setMessage("mysqlreader jdbcUrl database has too many!");
            failureApiResult.setContent("databases: " + collect);
            return failureApiResult;
        }
        String databaseName = collect.get(0);
        if (mysqlSimpleMapper.mysqlDbIsExist(databaseName) == 0) {
            failureApiResult.setMessage("mysqlreader jdbcUrl database is not exist!");
            failureApiResult.setContent("database: " + databaseName);
            return failureApiResult;
        }
        mysql2HiveDTO.getReader().getConnection().stream().map(MysqlReaderConnection::getTable).forEach(tableList::addAll);
        final List<String> notExistTables = new ArrayList<>(10);
        tableList.forEach(table -> {
            if (mysqlSimpleMapper.mysqlTblIsExist(databaseName, table) == 0) {
                notExistTables.add(table);
            }
        });
        if (!notExistTables.isEmpty()) {
            failureApiResult.setMessage("mysqlreader table does not exist!");
            failureApiResult.setContent("table not exist: " + notExistTables);
            return failureApiResult;
        }
        return ApiResult.initSuccess();
    }


}
