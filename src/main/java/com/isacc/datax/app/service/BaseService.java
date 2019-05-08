package com.isacc.datax.app.service;

import java.io.File;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.infra.config.DataxProperties;
import org.springframework.web.multipart.MultipartFile;

/**
 * <p>
 * Base Service
 * </p>
 *
 * @author isacc 2019/05/07 23:25
 */
public interface BaseService {

    /**
     * file转为MultipartFile
     *
     * @param file File
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019-05-05 20:44
     */
    ApiResult<Object> file2MultipartFile(File file);

    /**
     * 上传文件到datax服务器
     *
     * @param file            上传的文件
     * @param dataxProperties dataxProperties
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     */
    ApiResult<Object> uploadFile(MultipartFile file, DataxProperties dataxProperties);

    /**
     * 解析datax相关信息放入数组
     *
     * @param dataxProperties DataxProperties
     * @return java.lang.String[]
     * @author isacc 2019-05-07 10:21
     */
    String[] getDataxInfo(DataxProperties dataxProperties);

    /**
     * 远程执行python
     *
     * @param dataxProperties DataxProperties
     * @param jsonFileName    json file name
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019-05-07 10:06
     */
    ApiResult<Object> execCommand(DataxProperties dataxProperties, String jsonFileName);

}
