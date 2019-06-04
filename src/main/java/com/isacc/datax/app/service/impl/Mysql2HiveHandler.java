package com.isacc.datax.app.service.impl;

import java.util.*;
import java.util.stream.Collectors;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.DataxSyncDTO;
import com.isacc.datax.api.dto.HiveInfoDTO;
import com.isacc.datax.api.dto.Mysql2Hive;
import com.isacc.datax.app.service.DataxHandler;
import com.isacc.datax.app.service.HiveService;
import com.isacc.datax.domain.entity.datax.HivePartition;
import com.isacc.datax.domain.entity.datax.MysqlInfo;
import com.isacc.datax.domain.entity.reader.hdfsreader.HdfsColumn;
import com.isacc.datax.domain.entity.reader.hdfsreader.HdfsFileTypeEnum;
import com.isacc.datax.domain.entity.reader.mysqlreader.ReaderConnection;
import com.isacc.datax.infra.annotation.DataxHandlerType;
import com.isacc.datax.infra.config.DataxProperties;
import com.isacc.datax.infra.constant.Constants;
import com.isacc.datax.infra.constant.DataxHandlerTypeConstants;
import com.isacc.datax.infra.constant.DataxParameterConstants;
import com.isacc.datax.infra.mapper.MysqlSimpleMapper;
import com.isacc.datax.infra.util.DataxUtil;
import com.isacc.datax.infra.util.GenerateDataModelUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * <p>
 * description
 * </P>
 *
 * @author isacc 2019/05/23 11:42
 */
@Service
@Slf4j
@DataxHandlerType(DataxHandlerTypeConstants.MYSQL2HIVE)
public class Mysql2HiveHandler extends BaseDataxServiceImpl implements DataxHandler {

    private final DataxProperties dataxProperties;
    private final HiveService hiveService;
    private final MysqlSimpleMapper mysqlSimpleMapper;

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public Mysql2HiveHandler(DataxProperties dataxProperties, HiveService hiveService, MysqlSimpleMapper mysqlSimpleMapper) {
        this.dataxProperties = dataxProperties;
        this.hiveService = hiveService;
        this.mysqlSimpleMapper = mysqlSimpleMapper;
    }

    @Override
    public ApiResult<Object> handle(DataxSyncDTO dataxSyncDTO) {
        return this.mysql2Hive(dataxSyncDTO);
    }

    private ApiResult<Object> mysql2Hive(DataxSyncDTO dataxSyncDTO) {
        final Mysql2Hive mysql2Hive = dataxSyncDTO.getMysql2Hive();
        ApiResult<Object> failureResult = ApiResult.initFailure();
        if (!Optional.ofNullable(mysql2Hive).isPresent()) {
            failureResult.setMessage("DataxSyncDTO.mysql2Hive is null!");
            return failureResult;
        }
        ApiResult<Object> checkApiResult = this.checkCommonConfig(mysql2Hive);
        if (!checkApiResult.getResult()) {
            return checkApiResult;
        }
        // 判断是where还是querySql
        ApiResult<Object> generateJsonFileAndUploadResult;
        String jsonFileName = dataxSyncDTO.getJsonFileName();
        if (Optional.ofNullable(mysql2Hive.getReader().getWhere()).isPresent()) {
            // where模式
            final String whereTemplate = dataxProperties.getMysql2Hive().getWhereTemplate();
            Map<String, Object> dataModelWhere = this.generateDataModelMysql2HiveWhere(mysql2Hive);
            generateJsonFileAndUploadResult = this.generateJsonFileAndUpload(dataModelWhere, whereTemplate, jsonFileName, dataxProperties);
        } else {
            // querySql模式
            final String querySqlTemplate = dataxProperties.getMysql2Hive().getQuerySqlTemplate();
            Map<String, Object> dataModelQuerySql = this.generateDataModelMysql2HiveQuerySql(mysql2Hive);
            generateJsonFileAndUploadResult = this.generateJsonFileAndUpload(dataModelQuerySql, querySqlTemplate, jsonFileName, dataxProperties);
        }
        return generateJsonFileAndUploadResult;
    }

    /**
     * 生成mysql2hive_where的freemarker data model
     *
     * @param mysql2Hive Mysql2HiveDTO
     * @return java.util.Map<java.lang.String, java.lang.Object>
     * @author isacc 2019/5/22 14:25
     */
    private Map<String, Object> generateDataModelMysql2HiveWhere(Mysql2Hive mysql2Hive) {
        final HashMap<String, Object> root = new HashMap<>(16);
        // 通用的
        root.put(DataxParameterConstants.SETTING, mysql2Hive.getSetting());
        GenerateDataModelUtil.commonMysqlReader(root, mysql2Hive.getReader());
        // mysql 参数部分
        root.put(DataxParameterConstants.MYSQL_READER_COLUMN, mysql2Hive.getReader().getColumn());
        root.put(DataxParameterConstants.MYSQL_READER_WHERE, mysql2Hive.getReader().getWhere());
        //  hdfs 参数部分
        root.put(DataxParameterConstants.HDFS_WRITER_MODE, mysql2Hive.getWriter().getWriteMode());
        return GenerateDataModelUtil.commonHdfsWriter(root, mysql2Hive.getWriter());
    }

    /**
     * 生成mysql2hive_querySql的freemarker data model
     *
     * @param mysql2Hive Mysql2HiveDTO
     * @return java.util.Map<java.lang.String, java.lang.Object>
     * @author isacc 2019/5/22 14:25
     */
    private Map<String, Object> generateDataModelMysql2HiveQuerySql(Mysql2Hive mysql2Hive) {
        final HashMap<String, Object> root = new HashMap<>(16);
        // 通用
        root.put(DataxParameterConstants.SETTING, mysql2Hive.getSetting());
        GenerateDataModelUtil.commonMysqlReader(root, mysql2Hive.getReader());
        // mysql hdfs 参数部分
        root.put(DataxParameterConstants.HDFS_WRITER_MODE, mysql2Hive.getWriter().getWriteMode());
        return GenerateDataModelUtil.commonHdfsWriter(root, mysql2Hive.getWriter());
    }

    /**
     * 校验mysqlreader/hivewiter配置
     *
     * @param mysql2Hive Mysql2Hive
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019/5/10 16:56
     */
    private ApiResult<Object> checkCommonConfig(Mysql2Hive mysql2Hive) {
        ApiResult<Object> successApiResult = ApiResult.initSuccess();
        // 判断mysql的数据库/表是否存在，无则报错返回
        final ApiResult<Object> mysqlApiResult = this.mysqlDbAndTblIsExist(mysql2Hive);
        if (!mysqlApiResult.getResult()) {
            return mysqlApiResult;
        }
        MysqlInfo mysqlInfo = (MysqlInfo) mysqlApiResult.getContent();
        // 检验hiveWriter配置信息
        ApiResult<Object> checkApiResult = checkHiveWriterInfo(mysql2Hive, mysqlInfo);
        if (!checkApiResult.getResult()) {
            return checkApiResult;
        }
        successApiResult.setContent(mysqlInfo);
        return successApiResult;
    }

    /**
     * 检验hiveWriter配置信息
     *
     * @param mysql2Hive Mysql2Hive
     * @param mysqlInfo  MysqlInfo
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019/5/10 10:44
     */
    private ApiResult<Object> checkHiveWriterInfo(Mysql2Hive mysql2Hive, MysqlInfo mysqlInfo) {
        // 检验fileType fieldDelimiter(单字符) writeMode
        final ApiResult<Object> checkApiResult = this.checkHdfsParams(
                new String[]{mysql2Hive.getWriter().getFileType()},
                mysql2Hive.getWriter().getFieldDelimiter(),
                mysql2Hive.getWriter().getWriteMode());
        if (!checkApiResult.getResult()) {
            return checkApiResult;
        }
        // 判断hive的数据库/表是否存在，无则创建库表
        final ApiResult<Object> hiveDbAndTblIsResult = this.hiveDbAndTblIsExist(mysql2Hive, mysqlInfo);
        if (!hiveDbAndTblIsResult.getResult()) {
            return hiveDbAndTblIsResult;
        }
        return ApiResult.initSuccess();
    }

    /**
     * 校验hive的数据库、表是否存在
     *
     * @param mysql2Hive Mysql2Hive
     * @param mysqlInfo  MysqlInfo
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019-05-02 2:25
     */
    private ApiResult<Object> hiveDbAndTblIsExist(Mysql2Hive mysql2Hive, MysqlInfo mysqlInfo) {
        @NotBlank String path = mysql2Hive.getWriter().getPath();
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
        }
        // 是否存在都根据所选字段重新创建表
        List<HdfsColumn> hdfsColumns = generateHdfsColumn(mysql2Hive, mysqlInfo);
        HiveInfoDTO hiveInfoDTO = HiveInfoDTO.builder().
                databaseName(hiveDbName).
                tableName(hiveTblName).
                columns(hdfsColumns).
                fieldDelimiter(mysql2Hive.getWriter().getFieldDelimiter()).
                fileType(HdfsFileTypeEnum.valueOf(mysql2Hive.getWriter().getFileType().toUpperCase()).getFileType()).
                partitionList(partitionList).
                build();
        return this.hiveOps(path, hiveInfoDTO);
    }

    private ApiResult<Object> hiveOps(String path, HiveInfoDTO hiveInfoDTO) {
        ApiResult<Object> createTableApiResult = hiveService.createTable(hiveInfoDTO);
        // 若有分区，还需建分区
        if (!createTableApiResult.getResult()) {
            return createTableApiResult;
        }
        if (path.contains(Constants.Symbol.EQUAL)) {
            return hiveService.addPartition(hiveInfoDTO);
        }
        return ApiResult.initSuccess();
    }

    /**
     * 自动将mysql字段映射为hive字段类型
     *
     * @param mysql2Hive Mysql2Hive
     * @param mysqlInfo  MysqlInfo
     * @return java.util.List<com.isacc.datax.domain.entity.reader.hdfsreader.HdfsColumn>
     * @author isacc 2019/5/10 15:28
     */
    private List<HdfsColumn> generateHdfsColumn(Mysql2Hive mysql2Hive, MysqlInfo mysqlInfo) {
        ArrayList<HdfsColumn> hdfsColumns = new ArrayList<>();
        List<HdfsColumn> allColumns = new ArrayList<>();
        String databaseName = mysqlInfo.getDatabaseName();
        List<String> tableList = mysqlInfo.getTableList();
        tableList.forEach(table -> allColumns.addAll(mysqlSimpleMapper.mysqlColumn2HiveColumn(databaseName, table)));
        // 判断方式 是where 还是 querySql
        @NotEmpty List<ReaderConnection> connections = mysql2Hive.getReader().getConnection();
        String where = mysql2Hive.getReader().getWhere();
        if (!Objects.isNull(where)) {
            // where方式
            @NotEmpty List<String> columns = mysql2Hive.getReader().getColumn();
            allColumns.forEach(hdfsColumn -> columns.forEach(column -> {
                if (column.equalsIgnoreCase(hdfsColumn.getName())) {
                    hdfsColumns.add(hdfsColumn);
                }
            }));
            return hdfsColumns;
        }
        // querySql方式
        connections.forEach(conn -> conn.getQuerySql().forEach(querySql -> {
            List<String> columns = Arrays.asList(querySql.substring(0, querySql.indexOf("FROM")).replace("SELECT", "").trim().split(","));
            allColumns.forEach(hdfsColumn -> columns.forEach(column -> {
                if (column.trim().equalsIgnoreCase(hdfsColumn.getName())) {
                    hdfsColumns.add(hdfsColumn);
                }
            }));
        }));
        return hdfsColumns;
    }

    /**
     * 校验mysql的数据库、表是否存在
     *
     * @param mysql2Hive Mysql2Hive
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019-04-29 21:11
     */
    private ApiResult<Object> mysqlDbAndTblIsExist(Mysql2Hive mysql2Hive) {
        ApiResult<Object> successApiResult = ApiResult.initSuccess();
        final ApiResult<Object> failureApiResult = ApiResult.initFailure();
        // 检验库
        ApiResult<Object> checkMysqlDatabaseResult = this.checkMysqlDatabase(mysql2Hive);
        if (!checkMysqlDatabaseResult.getResult()) {
            return checkMysqlDatabaseResult;
        }
        String databaseName = String.valueOf(checkMysqlDatabaseResult.getContent());
        if (mysqlSimpleMapper.mysqlDbIsExist(databaseName) == 0) {
            failureApiResult.setMessage("mysqlreader jdbcUrl database is not exist!");
            failureApiResult.setContent("database: " + databaseName);
            return failureApiResult;
        }
        // 校验表
        ApiResult<Object> checkMysqlTableResult = this.checkMysqlTable(mysql2Hive, databaseName);
        if (!checkMysqlTableResult.getResult()) {
            return checkMysqlTableResult;
        }
        final List<String> tableList = ((MysqlInfo) checkMysqlTableResult.getContent()).getTableList();
        successApiResult.setContent(MysqlInfo.builder().databaseName(databaseName).tableList(tableList).build());
        return successApiResult;
    }

    /**
     * 检验mysqlreader数据库
     *
     * @param mysql2Hive Mysql2Hive
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019/5/10 16:38
     */
    private ApiResult<Object> checkMysqlDatabase(Mysql2Hive mysql2Hive) {
        final ApiResult<Object> successApiResult = ApiResult.initSuccess();
        final ApiResult<Object> failureApiResult = ApiResult.initFailure();
        final List<String> databaseNameList = new ArrayList<>(5);
        mysql2Hive.getReader().getConnection().stream().map(ReaderConnection::getJdbcUrl).forEach(jdbcUrls -> jdbcUrls.forEach(url ->
                databaseNameList.add(url.substring(url.lastIndexOf('/') + 1, !url.contains("?") ? url.length() : url.indexOf('?')))
        ));
        List<String> collect = databaseNameList.stream().distinct().collect(Collectors.toList());
        if (collect.size() != 1) {
            failureApiResult.setMessage("mysqlreader jdbcUrl database has too many!");
            failureApiResult.setContent("databases: " + collect);
            return failureApiResult;
        }
        String databaseName = collect.get(0);
        successApiResult.setContent(databaseName);
        return successApiResult;
    }

    /**
     * 检验mysqlreader数据下的表
     *
     * @param mysql2Hive   Mysql2Hive
     * @param databaseName 数据库名称
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019/5/10 16:44
     */
    private ApiResult<Object> checkMysqlTable(Mysql2Hive mysql2Hive, String databaseName) {
        final ApiResult<Object> successApiResult = ApiResult.initSuccess();
        final ApiResult<Object> failureApiResult = ApiResult.initFailure();
        final List<String> tableList = new ArrayList<>(10);
        String where = mysql2Hive.getReader().getWhere();
        if (!Objects.isNull(where)) {
            // where 方式
            mysql2Hive.getReader().getConnection().stream().map(ReaderConnection::getTable).forEach(tableList::addAll);
        } else {
            // querySql方式
            mysql2Hive.getReader().getConnection().stream().map(ReaderConnection::getQuerySql).forEach(querySqlList ->
                    querySqlList.forEach(querySql ->
                            tableList.add(querySql.substring(querySql.indexOf("FROM")).replace("FROM", "").replace(";", "").trim())
                    ));
        }
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
        successApiResult.setContent(MysqlInfo.builder().tableList(tableList).build());
        return successApiResult;
    }

}
