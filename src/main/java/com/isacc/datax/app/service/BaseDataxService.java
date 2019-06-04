package com.isacc.datax.app.service;

import java.util.Map;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.DataxSyncDTO;
import com.isacc.datax.infra.config.AzkabanProperties;
import com.isacc.datax.infra.config.DataxProperties;


/**
 * <p>
 * description
 * </P>
 *
 * @author isacc 2019/05/18 0:47
 */
public interface BaseDataxService {

    /**
     * 生成json文件，上传到datax服务器
     *
     * @param dataModel       freemarker data model
     * @param template        模板名称
     * @param jsonFileName    datax生成的json file名称
     * @param dataxProperties Datax相关信息
     * @return com.hand.hdsp.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019/5/18 1:00
     */
    ApiResult<Object> generateJsonFileAndUpload(Map<String, Object> dataModel, String template, String jsonFileName, DataxProperties dataxProperties);

    /**
     * 生成azkaban创建项目的zip
     *
     * @param jsonFileName      datax json file name
     * @param azkabanProperties AzkabanProperties
     * @param dataxProperties   DataxProperties
     * @return com.hand.hdsp.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019/5/20 23:42
     */
    ApiResult<Object> generateAzkabanZip(String jsonFileName, AzkabanProperties azkabanProperties, DataxProperties dataxProperties);

    /**
     * 写datax json文件信息到表
     *
     * @param dataxSyncDTO      DataxSyncDTO
     * @param dataxProperties   DataxProperties
     * @param azkabanProperties AzkabanProperties
     * @return com.hand.hdsp.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019/5/21 13:38
     */
    ApiResult<Object> writeDataxSettingInfo(DataxSyncDTO dataxSyncDTO, DataxProperties dataxProperties, AzkabanProperties azkabanProperties);

    /**
     * 删除datax服务器上json文件
     *
     * @param dataxProperties DataxProperties
     * @param dataxSyncDTO    DataxSyncDTO
     * @return com.hand.hdsp.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019/5/21 15:29
     */
    ApiResult<Object> deleteDataxJsonFile(DataxProperties dataxProperties, DataxSyncDTO dataxSyncDTO);

}
