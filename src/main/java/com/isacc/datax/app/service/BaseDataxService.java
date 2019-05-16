package com.isacc.datax.app.service;

import java.util.Map;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.infra.config.AzkabanProperties;
import com.isacc.datax.infra.config.DataxProperties;

/**
 * <p>
 * Base Service
 * </p>
 *
 * @author isacc 2019/05/07 23:25
 */
public interface BaseDataxService {

    /**
     * azkaban调度进行datax导数
     *
     * @param dataModel         freemarker data model
     * @param dataxProperties   DataxProperties
     * @param templateName      templateName
     * @param azkabanProperties azkabanProperties
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author headers 2019/5/15 14:34
     */
    ApiResult<Object> startDataExtraction(Map<String, Object> dataModel, DataxProperties dataxProperties,
                                          String templateName, AzkabanProperties azkabanProperties);

}
