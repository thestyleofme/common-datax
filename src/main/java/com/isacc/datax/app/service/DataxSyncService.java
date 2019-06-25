package com.isacc.datax.app.service;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.DataxSyncDTO;

/**
 * 数据同步表应用服务
 *
 * @author isacc 2019-05-17 14:07:48
 */
public interface DataxSyncService extends BaseDataxService {

    /**
     * 执行datax数据同步
     *
     * @param dataxSyncDTO DataxSyncDTO
     * @return com.hand.hdsp.datax.api.dto.ApiResult<java.lang.Object>
     * @author HP_USER 2019/5/23 11:24
     */
    ApiResult<Object> execute(DataxSyncDTO dataxSyncDTO);

    /**
     * 生成datax任务执行命令
     *
     * @return java.lang.String
     * @author isacc 2019/5/31 11:47
     */
    String generateDataxCommand();

    /**
     * 删除datax同步任务
     *
     * @param dataxSyncDTO DataxSyncDTO
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019/6/25 16:30
     */
    ApiResult<Object> deleteDataxSync(DataxSyncDTO dataxSyncDTO);

    /**
     * 校验datax同步任务名称以及json文件名称是否重复
     *
     * @param dataxSyncDTO DataxSyncDTO
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019/6/25 16:33
     */
    ApiResult<Object> checkSyncNameAndJsonFileName(DataxSyncDTO dataxSyncDTO);
}
