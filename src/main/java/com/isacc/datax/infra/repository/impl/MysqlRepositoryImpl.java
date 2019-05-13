package com.isacc.datax.infra.repository.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.validation.constraints.NotBlank;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.Hive2HiveDTO;
import com.isacc.datax.api.dto.HiveInfoDTO;
import com.isacc.datax.domain.repository.MysqlRepository;
import com.isacc.datax.infra.constant.Constants;
import com.isacc.datax.infra.mapper.MysqlSimpleMapper;
import com.isacc.datax.infra.util.DataxUtil;
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

    @Autowired
    public MysqlRepositoryImpl(MysqlSimpleMapper mysqlSimpleMapper) {
        this.mysqlSimpleMapper = mysqlSimpleMapper;
    }

    @Override
    public ApiResult<Object> checkHiveDbAndTable(Hive2HiveDTO hive2HiveDTO) {
        // reader
        @NotBlank final String readerPath = hive2HiveDTO.getReader().getPath();
        final ApiResult<Object> readerApiResult = this.justCheckHiveDbAndTbl(readerPath);
        if (!readerApiResult.getResult()) {
            return readerApiResult;
        }
        // writer
        return checkWriterHiveDbAndTable(hive2HiveDTO);
    }

    @Override
    public ApiResult<Object> checkWriterHiveDbAndTable(Hive2HiveDTO hive2HiveDTO) {
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
        HiveInfoDTO hiveInfo = (HiveInfoDTO) DataxUtil.getHiveInfoFromPath(path).getContent();
        String hiveDbName = hiveInfo.getDatabaseName();
        String hiveTblName = hiveInfo.getTableName();
        final Map<String, Object> hiveDbInfoMap = mysqlSimpleMapper.hiveDbIsExist(hiveDbName);
        final HashMap<String, Object> map = new HashMap<>(3);
        map.put("hiveDbName", hiveDbName);
        map.put("hiveTblName", hiveTblName);
        if (Objects.isNull(hiveDbInfoMap)) {
            final ApiResult<Object> failureApiResult = ApiResult.initFailure();
            failureApiResult.setMessage(String.format("path路径错误，不存在该hive数据库：%s!", hiveDbName));
            map.put("errorType", Constants.DB_IS_NOT_EXIST);
            failureApiResult.setContent(map);
            return failureApiResult;
        }
        final Long dbId = Long.valueOf(String.valueOf(hiveDbInfoMap.get("DB_ID")));
        final Map<String, Object> hiveTblInfoMap = mysqlSimpleMapper.hiveTblIsExist(dbId, hiveTblName);
        if (Objects.isNull(hiveTblInfoMap)) {
            final ApiResult<Object> failureApiResult = ApiResult.initFailure();
            failureApiResult.setMessage(String.format("path路径错误，%s数据库下不存在表：%s!", hiveDbName, hiveTblName));
            map.put("errorType", Constants.TBL_IS_NOT_EXIST);
            failureApiResult.setContent(map);
            return failureApiResult;
        }
        return ApiResult.initSuccess();
    }

}
