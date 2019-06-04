package com.isacc.datax.app.service;


import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.DataxSyncDTO;

/**
 * <p>
 * description
 * </P>
 *
 * @author isacc 2019/05/23 9:23
 */
public interface DataxHandler {

    /**
     * 按照datax同步任务类型进行相应处理
     *
     * @param dataxSyncDTO DataxSyncDTO
     * @return com.hand.hdsp.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019/5/23 9:24
     */
    ApiResult<Object> handle(DataxSyncDTO dataxSyncDTO);

}
