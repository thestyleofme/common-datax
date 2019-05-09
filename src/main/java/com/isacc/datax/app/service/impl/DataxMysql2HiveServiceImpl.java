package com.isacc.datax.app.service.impl;

import java.util.*;
import java.util.stream.Collectors;
import javax.validation.constraints.NotBlank;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.HiveInfoDTO;
import com.isacc.datax.api.dto.Mysql2HiveDTO;
import com.isacc.datax.app.service.DataxMysql2HiveService;
import com.isacc.datax.app.service.HiveService;
import com.isacc.datax.domain.entity.datax.HivePartition;
import com.isacc.datax.domain.entity.reader.hdfsreader.HdfsFileTypeEnum;
import com.isacc.datax.domain.entity.reader.mysqlreader.MysqlReaderConnection;
import com.isacc.datax.infra.config.DataxProperties;
import com.isacc.datax.infra.constant.Constants;
import com.isacc.datax.infra.mapper.MysqlSimpleMapper;
import com.isacc.datax.infra.util.DataxUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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

    @Override
    public ApiResult<Object> mysql2HiveWhere(Mysql2HiveDTO mysql2HiveDTO) {
        // 判断mysql的数据库/表是否存在，无则报错返回
        final ApiResult<Object> mysqlApiResult = this.mysqlDbAndTblIsExist(mysql2HiveDTO);
        if (!mysqlApiResult.getResult()) {
            return mysqlApiResult;
        }
        // 检验fileType fieldDelimiter(单字符) writeMode
        final ApiResult<Object> checkApiResult = this.checkHdfsParams(
                new String[]{mysql2HiveDTO.getWriter().getFileType()},
                mysql2HiveDTO.getWriter().getFieldDelimiter(),
                mysql2HiveDTO.getWriter().getWriteMode());
        if (!checkApiResult.getResult()) {
            return checkApiResult;
        }
        // 判断hive的数据库/表是否存在，无则创建库表
        final ApiResult<Object> hiveApiResult = this.hiveDbAndTblIsExist(mysql2HiveDTO);
        if (!hiveApiResult.getResult()) {
            return hiveApiResult;
        }
        // 开始导数
        final String whereTemplate = dataxProperties.getMysql2Hive().getWhereTemplate();
        final Map<String, Object> dataModel = generateDataModel(mysql2HiveDTO);
        return this.afterCheckOperations(dataModel, dataxProperties, whereTemplate);
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
     * 校验hive的数据库、表是否存在
     *
     * @param mysql2HiveDTO Mysql2HiveDTO
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019-05-02 2:25
     */
    private ApiResult<Object> hiveDbAndTblIsExist(Mysql2HiveDTO mysql2HiveDTO) {
        @NotBlank String path = mysql2HiveDTO.getWriter().getPath();
        // 判断path是否含有分区信息
        List<HivePartition> partitionList = DataxUtil.partitionList(path);
        HiveInfoDTO hiveInfo = (HiveInfoDTO) DataxUtil.getHiveInfoFromPath(path).getContent();
        String hiveDbName = hiveInfo.getDatabaseName();
        String hiveTblName = hiveInfo.getTableName();
        final Map<String, Object> hiveDbInfoMap = mysqlSimpleMapper.hiveDbIsExist(hiveDbName);
        if (Objects.isNull(hiveDbInfoMap)) {
            // 不存在hive数据库，先创建库，再根据所选字段创建表
            final ApiResult<Object> createDbApiResult = hiveService.createDatabase(hiveDbName);
            if (!createDbApiResult.getResult()) {
                return createDbApiResult;
            }
        } else {
            // 存在hive数据库但不存在表，根据所选字段创建表
            HiveInfoDTO hiveInfoDTO = HiveInfoDTO.builder().
                    databaseName(hiveDbName).
                    tableName(hiveTblName).
                    columns(mysql2HiveDTO.getWriter().getColumn()).
                    fieldDelimiter(mysql2HiveDTO.getWriter().getFieldDelimiter()).
                    fileType(HdfsFileTypeEnum.valueOf(mysql2HiveDTO.getWriter().getFileType().toUpperCase()).getFileType()).
                    partitionList(partitionList).
                    build();
            ApiResult<Object> createTableApiResult = hiveService.createTable(hiveInfoDTO);
            // 若有分区，还需建分区
            if (!createTableApiResult.getResult()) {
                return createTableApiResult;
            }
            if (path.contains(Constants.Symbol.EQUAL)) {
                return hiveService.addPartition(hiveInfoDTO);
            }
        }
        return ApiResult.initSuccess();
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
