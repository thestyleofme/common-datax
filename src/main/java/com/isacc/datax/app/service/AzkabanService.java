package com.isacc.datax.app.service;

import com.isacc.datax.api.dto.ApiResult;

/**
 * <p>
 * description
 * </P>
 *
 * @author isacc 2019/05/13 22:02
 */
public interface AzkabanService {

    /**
     * azkaban执行datax job
     *
     * @param projectName 要创建的项目名称
     * @param description 要创建的项目描述
     * @param zipPath     要上传的zip包路径
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019/5/14 20:33
     */
    ApiResult<Object> executeDataxJob(String projectName, String description, String zipPath);

}
