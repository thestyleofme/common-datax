package com.isacc.datax.app.service;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.Mysql2HiveDTO;

/**
 * <p>
 * DataX Service
 * </p>
 *
 * @author isacc 2019/04/29 17:05
 */
public interface DataxMysql2HiveService {

    /**
     * Mysql导数到Hive
     *
     * @param mysql2HiveDTO Mysql2HiveDTO
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019-04-29 17:08
     */
    ApiResult<Object> mysql2HiveWhere(Mysql2HiveDTO mysql2HiveDTO);


}
