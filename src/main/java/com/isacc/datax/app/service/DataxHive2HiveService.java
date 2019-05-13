package com.isacc.datax.app.service;


import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.Hive2HiveDTO;

/**
 * <p>
 * DataX Hive2Hive Service
 * </p>
 *
 * @author isacc 2019/05/07 14:14
 */
public interface DataxHive2HiveService {

    /**
     * Datax hive之间的导数
     *
     * @param hive2HiveDTO Hive2HiveDTO
     * @param source       如csv上传时需要传，本地csv文件路径
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019-05-07 14:16
     */
    ApiResult<Object> hive2hive(Hive2HiveDTO hive2HiveDTO, String source);

}
