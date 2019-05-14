package com.isacc.datax.app.service.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.HiveInfoDTO;
import com.isacc.datax.api.dto.Mysql2HiveDTO;
import com.isacc.datax.app.service.AzkabanService;
import com.isacc.datax.app.service.DataxMysql2HiveService;
import com.isacc.datax.app.service.HiveService;
import com.isacc.datax.domain.entity.datax.HivePartition;
import com.isacc.datax.domain.entity.datax.MysqlInfo;
import com.isacc.datax.domain.entity.reader.hdfsreader.HdfsColumn;
import com.isacc.datax.domain.entity.reader.hdfsreader.HdfsFileTypeEnum;
import com.isacc.datax.domain.entity.reader.mysqlreader.MysqlReaderConnection;
import com.isacc.datax.infra.config.AzkabanProperties;
import com.isacc.datax.infra.config.DataxProperties;
import com.isacc.datax.infra.constant.Constants;
import com.isacc.datax.infra.mapper.MysqlSimpleMapper;
import com.isacc.datax.infra.util.DataxUtil;
import com.isacc.datax.infra.util.ZipUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
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
    private final AzkabanProperties azkabanProperties;
    private final AzkabanService azkabanService;

    @Autowired
    public DataxMysql2HiveServiceImpl(MysqlSimpleMapper mysqlSimpleMapper, HiveService hiveService, DataxProperties dataxProperties, AzkabanProperties azkabanProperties, AzkabanService azkabanService) {
        this.mysqlSimpleMapper = mysqlSimpleMapper;
        this.hiveService = hiveService;
        this.dataxProperties = dataxProperties;
        this.azkabanProperties = azkabanProperties;
        this.azkabanService = azkabanService;
    }

    @Override
    public ApiResult<Object> mysql2HiveWhere(Mysql2HiveDTO mysql2HiveDTO) {
        ApiResult<Object> checkApiResult = this.checkCommonConfig(mysql2HiveDTO);
        if (!checkApiResult.getResult()) {
            return checkApiResult;
        }
        // 生成json文件，上传到datax服务器
        MysqlInfo mysqlInfo = (MysqlInfo) checkApiResult.getContent();
        final String whereTemplate = dataxProperties.getMysql2Hive().getWhereTemplate();
        final Map<String, Object> dataModel = generateDataModelWhere(mysql2HiveDTO, mysqlInfo);
        ApiResult<Object> uploadResult = this.startDataExtraction(dataModel, dataxProperties, whereTemplate);
        if (!uploadResult.getResult()) {
            return uploadResult;
        }
        // azkaban进行调度
        String fileName = String.valueOf(uploadResult.getContent());
        String dataxParamProperties = azkabanProperties.getLocalDicPath() + azkabanProperties.getDataxProperties();
        // 生成dataxParams.properties
        Properties properties = new Properties();
        try (FileOutputStream fos = new FileOutputStream(dataxParamProperties)) {
            properties.setProperty("DATAX_HOME", dataxProperties.getHome());
            properties.setProperty("DATAX_UPLOAD_DIC", dataxProperties.getUploadDicPath());
            properties.setProperty("DATAX_JSON_FILE_NAME", fileName);
            properties.store(fos, "datax properties");
        } catch (IOException e) {
            ApiResult<Object> failureResult = ApiResult.initFailure();
            log.error("dataxParams.properties生成失败！", e);
            failureResult.setMessage("IOException: " + e.getMessage());
            return failureResult;
        }
        // 压缩dataxParams.properties和json file
        ArrayList<File> files = new ArrayList<>();
        files.add(new File(dataxParamProperties));
        files.add(new File(azkabanProperties.getDataxJob()));
        try (FileOutputStream zipOut = new FileOutputStream(azkabanProperties.getLocalDicPath() + "dataxJob.zip")) {
            ZipUtils.toZip(files, zipOut);
            // 压缩过后删除dataxParams.properties
            FileUtils.deleteQuietly(new File(dataxParamProperties));
        } catch (IOException e) {
            ApiResult<Object> failureResult = ApiResult.initFailure();
            log.error("dataxJob.zip generate error！", e);
            failureResult.setMessage("IOException: " + e.getMessage());
            return failureResult;
        }
        // azkaban操作
        return azkabanService.executeDataxJob(ZipUtils.generateFileName("dataxJob"),
                "datax job",
                azkabanProperties.getLocalDicPath() + ZipUtils.generateFileName("dataxJob") + ".zip");
    }

    @Override
    public ApiResult<Object> mysql2HiveQuerySql(Mysql2HiveDTO mysql2HiveDTO) {
        ApiResult<Object> checkApiResult = this.checkCommonConfig(mysql2HiveDTO);
        if (!checkApiResult.getResult()) {
            return checkApiResult;
        }
        // 开始导数
        MysqlInfo mysqlInfo = (MysqlInfo) checkApiResult.getContent();
        final String querySqlTemplate = dataxProperties.getMysql2Hive().getQuerySqlTemplate();
        final Map<String, Object> dataModel = generateDataModelQuerySql(mysql2HiveDTO, mysqlInfo);
        return this.startDataExtraction(dataModel, dataxProperties, querySqlTemplate);
    }

    /**
     * 校验mysqlreader/hivewiter配置
     *
     * @param mysql2HiveDTO Mysql2HiveDTO
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019/5/10 16:56
     */
    private ApiResult<Object> checkCommonConfig(Mysql2HiveDTO mysql2HiveDTO) {
        ApiResult<Object> successApiResult = ApiResult.initSuccess();
        // 判断mysql的数据库/表是否存在，无则报错返回
        final ApiResult<Object> mysqlApiResult = this.mysqlDbAndTblIsExist(mysql2HiveDTO);
        if (!mysqlApiResult.getResult()) {
            return mysqlApiResult;
        }
        MysqlInfo mysqlInfo = (MysqlInfo) mysqlApiResult.getContent();
        // 检验hiveWriter配置信息
        ApiResult<Object> checkApiResult = checkHiveWriterInfo(mysql2HiveDTO, mysqlInfo);
        if (!checkApiResult.getResult()) {
            return checkApiResult;
        }
        successApiResult.setContent(mysqlInfo);
        return successApiResult;
    }

    /**
     * 检验hiveWriter配置信息
     *
     * @param mysql2HiveDTO Mysql2HiveDTO
     * @param mysqlInfo     MysqlInfo
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019/5/10 10:44
     */
    private ApiResult<Object> checkHiveWriterInfo(Mysql2HiveDTO mysql2HiveDTO, MysqlInfo mysqlInfo) {
        // 检验fileType fieldDelimiter(单字符) writeMode
        final ApiResult<Object> checkApiResult = this.checkHdfsParams(
                new String[]{mysql2HiveDTO.getWriter().getFileType()},
                mysql2HiveDTO.getWriter().getFieldDelimiter(),
                mysql2HiveDTO.getWriter().getWriteMode());
        if (!checkApiResult.getResult()) {
            return checkApiResult;
        }
        // 判断hive的数据库/表是否存在，无则创建库表
        final ApiResult<Object> hiveApiResult = this.hiveDbAndTblIsExist(mysql2HiveDTO, mysqlInfo);
        if (!hiveApiResult.getResult()) {
            return hiveApiResult;
        }
        return ApiResult.initSuccess();
    }

    /**
     * 创建querySql freemarker的DataModel
     *
     * @param mysql2HiveDTO Mysql2HiveDTO
     * @return java.util.Map<java.lang.String, java.lang.Object>
     * @author isacc 2019-05-05 16:04
     */
    private Map<String, Object> generateDataModelQuerySql(Mysql2HiveDTO mysql2HiveDTO, MysqlInfo mysqlInfo) {
        final HashMap<String, Object> root = new HashMap<>(16);
        // setting
        root.put("setting", mysql2HiveDTO.getSetting());
        // mysql
        root.put("username", mysql2HiveDTO.getReader().getUsername());
        root.put("password", mysql2HiveDTO.getReader().getPassword());
        root.put("connection", mysql2HiveDTO.getReader().getConnection());
        // hdfs
        return getHdfsWriterModel(mysql2HiveDTO, mysqlInfo, root);
    }

    /**
     * hdfs通用的配置提取
     *
     * @param mysql2HiveDTO Mysql2HiveDTO
     * @param mysqlInfo     MysqlInfo
     * @param root          Freemarker dataModel
     * @return java.util.Map<java.lang.String, java.lang.Object>
     * @author isacc 2019/5/10 10:54
     */
    private Map<String, Object> getHdfsWriterModel(Mysql2HiveDTO mysql2HiveDTO, MysqlInfo mysqlInfo, HashMap<String, Object> root) {
        root.put("hdfsColumn", generateHdfsColumn(mysql2HiveDTO, mysqlInfo));
        root.put("defaultFS", mysql2HiveDTO.getWriter().getDefaultFS());
        root.put("fileType", mysql2HiveDTO.getWriter().getFileType());
        root.put("path", mysql2HiveDTO.getWriter().getPath());
        root.put("fileName", mysql2HiveDTO.getWriter().getFileName());
        root.put("writeMode", mysql2HiveDTO.getWriter().getWriteMode());
        root.put("fieldDelimiter", mysql2HiveDTO.getWriter().getFieldDelimiter());
        return root;
    }

    /**
     * 创建where freemarker的DataModel
     *
     * @param mysql2HiveDTO Mysql2HiveDTO
     * @return java.util.Map<java.lang.String, java.lang.Object>
     * @author isacc 2019-05-05 16:04
     */
    private Map<String, Object> generateDataModelWhere(Mysql2HiveDTO mysql2HiveDTO, MysqlInfo mysqlInfo) {
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
        return getHdfsWriterModel(mysql2HiveDTO, mysqlInfo, root);
    }

    /**
     * 校验hive的数据库、表是否存在
     *
     * @param mysql2HiveDTO Mysql2HiveDTO
     * @param mysqlInfo     MysqlInfo
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019-05-02 2:25
     */
    private ApiResult<Object> hiveDbAndTblIsExist(Mysql2HiveDTO mysql2HiveDTO, MysqlInfo mysqlInfo) {
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
        }
        // 是否存在都根据所选字段重新创建表
        List<HdfsColumn> hdfsColumns = generateHdfsColumn(mysql2HiveDTO, mysqlInfo);
        HiveInfoDTO hiveInfoDTO = HiveInfoDTO.builder().
                databaseName(hiveDbName).
                tableName(hiveTblName).
                columns(hdfsColumns).
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
        return ApiResult.initSuccess();
    }

    /**
     * 自动将mysql字段映射为hive字段类型
     *
     * @param mysql2HiveDTO Mysql2HiveDTO
     * @param mysqlInfo     MysqlInfo
     * @return java.util.List<com.isacc.datax.domain.entity.reader.hdfsreader.HdfsColumn>
     * @author isacc 2019/5/10 15:28
     */
    private List<HdfsColumn> generateHdfsColumn(Mysql2HiveDTO mysql2HiveDTO, MysqlInfo mysqlInfo) {
        ArrayList<HdfsColumn> hdfsColumns = new ArrayList<>();
        List<HdfsColumn> allColumns = new ArrayList<>();
        String databaseName = mysqlInfo.getDatabaseName();
        List<String> tableList = mysqlInfo.getTableList();
        tableList.forEach(table -> allColumns.addAll(mysqlSimpleMapper.mysqlColumn2HiveColumn(databaseName, table)));
        // 判断方式 是where 还是 querySql
        @NotEmpty List<MysqlReaderConnection> connections = mysql2HiveDTO.getReader().getConnection();
        String where = mysql2HiveDTO.getReader().getWhere();
        if (!Objects.isNull(where)) {
            // where方式
            @NotEmpty List<String> columns = mysql2HiveDTO.getReader().getColumn();
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
     * 检验mysqlreader数据库
     *
     * @param mysql2HiveDTO Mysql2HiveDTO
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019/5/10 16:38
     */
    private ApiResult<Object> checkMysqlDatabase(Mysql2HiveDTO mysql2HiveDTO) {
        final ApiResult<Object> successApiResult = ApiResult.initSuccess();
        final ApiResult<Object> failureApiResult = ApiResult.initFailure();
        final List<String> databaseNameList = new ArrayList<>(5);
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
        successApiResult.setContent(databaseName);
        return successApiResult;
    }

    /**
     * 检验mysqlreader数据下的表
     *
     * @param mysql2HiveDTO Mysql2HiveDTO
     * @param databaseName  数据库名称
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019/5/10 16:44
     */
    private ApiResult<Object> checkMysqlTable(Mysql2HiveDTO mysql2HiveDTO, String databaseName) {
        final ApiResult<Object> successApiResult = ApiResult.initSuccess();
        final ApiResult<Object> failureApiResult = ApiResult.initFailure();
        final List<String> tableList = new ArrayList<>(10);
        String where = mysql2HiveDTO.getReader().getWhere();
        if (!Objects.isNull(where)) {
            // where 方式
            mysql2HiveDTO.getReader().getConnection().stream().map(MysqlReaderConnection::getTable).forEach(tableList::addAll);
        } else {
            // querySql方式
            mysql2HiveDTO.getReader().getConnection().stream().map(MysqlReaderConnection::getQuerySql).forEach(querySqlList -> querySqlList.forEach(querySql -> {
                tableList.add(querySql.substring(querySql.indexOf("FROM")).replace("FROM", "").replace(";", "").trim());
            }));
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

    /**
     * 校验mysql的数据库、表是否存在
     *
     * @param mysql2HiveDTO Mysql2HiveDTO
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019-04-29 21:11
     */
    private ApiResult<Object> mysqlDbAndTblIsExist(Mysql2HiveDTO mysql2HiveDTO) {
        ApiResult<Object> successApiResult = ApiResult.initSuccess();
        final ApiResult<Object> failureApiResult = ApiResult.initFailure();
        // 检验库
        ApiResult<Object> checkMysqlDatabaseResult = this.checkMysqlDatabase(mysql2HiveDTO);
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
        ApiResult<Object> checkMysqlTableResult = this.checkMysqlTable(mysql2HiveDTO, databaseName);
        if (!checkMysqlTableResult.getResult()) {
            return checkMysqlTableResult;
        }
        final List<String> tableList = ((MysqlInfo) checkMysqlTableResult.getContent()).getTableList();
        successApiResult.setContent(MysqlInfo.builder().databaseName(databaseName).tableList(tableList).build());
        return successApiResult;
    }


}
