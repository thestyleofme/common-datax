package com.isacc.datax.infra.repository.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.validation.constraints.NotBlank;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.Hive2HiveDTO;
import com.isacc.datax.domain.repository.MysqlRepository;
import com.isacc.datax.infra.mapper.MysqlSimpleMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * <p>
 * Mysql Repository Impl
 * </p>
 *
 * @author isacc 2019/04/29 19:49
 */
@SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
@Component
@Slf4j
public class MysqlRepositoryImpl implements MysqlRepository {

    private final MysqlSimpleMapper mysqlSimpleMapper;

    private static final String DB_IS_NOT_EXIST = "DB_IS_NOT_EXIST";
    private static final String TBL_IS_NOT_EXIST = "TBL_IS_NOT_EXIST";

    @Autowired
    public MysqlRepositoryImpl(MysqlSimpleMapper mysqlSimpleMapper) {
        this.mysqlSimpleMapper = mysqlSimpleMapper;
    }

    @Override
    public ApiResult<Object> hiveDbAndTableIsExist(Hive2HiveDTO hive2HiveDTO) {
        // reader
        @NotBlank final String readerPath = hive2HiveDTO.getReader().getPath();
        final ApiResult<Object> readerApiResult = this.justCheckHiveDbAndTbl(readerPath);
        if (!readerApiResult.getResult()) {
            return readerApiResult;
        }
        // writer
        @NotBlank final String writerPath = hive2HiveDTO.getWriter().getPath();
        final ApiResult<Object> writerApiResult = this.justCheckHiveDbAndTbl(writerPath);
        if (!writerApiResult.getResult()) {
            // 改变状态，返回再接着操作
            writerApiResult.setResult(true);
            return writerApiResult;
        }
        return ApiResult.initSuccess();
    }

    private ApiResult<Object> justCheckHiveDbAndTbl(String path) {
        String hivePath = path.substring(0, path.lastIndexOf('/'));
        String hiveDbName = hivePath.substring(hivePath.lastIndexOf('/') + 1, hivePath.indexOf('.'));
        final Map<String, Object> hiveDbInfoMap = mysqlSimpleMapper.hiveDbIsExist(hiveDbName);
        String hiveTblName = path.substring(path.lastIndexOf('/') + 1);
        final HashMap<String, Object> map = new HashMap<>(3);
        map.put("hiveDbName", hiveDbName);
        map.put("hiveTblName", hiveTblName);
        if (Objects.isNull(hiveDbInfoMap)) {
            final ApiResult<Object> failureApiResult = ApiResult.initFailure();
            failureApiResult.setMessage(String.format("path路径错误，不存在该hive数据库：%s!", hiveDbName));
            map.put("errorType", DB_IS_NOT_EXIST);
            failureApiResult.setContent(map);
            return failureApiResult;
        }
        final Long dbId = Long.valueOf(String.valueOf(hiveDbInfoMap.get("DB_ID")));
        final Map<String, Object> hiveTblInfoMap = mysqlSimpleMapper.hiveTblIsExist(dbId, hiveTblName);
        if (Objects.isNull(hiveTblInfoMap)) {
            final ApiResult<Object> failureApiResult = ApiResult.initFailure();
            failureApiResult.setMessage(String.format("path路径错误，%s数据库下不存在表：%s!", hiveDbName, hiveTblName));
            map.put("errorType", TBL_IS_NOT_EXIST);
            failureApiResult.setContent(map);
            return failureApiResult;
        }
        return ApiResult.initSuccess();
    }

}
