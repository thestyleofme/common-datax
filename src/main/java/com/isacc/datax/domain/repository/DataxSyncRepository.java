package com.isacc.datax.domain.repository;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.isacc.datax.api.dto.DataxSyncDTO;
import com.isacc.datax.domain.entity.DataxSync;

/**
 * 数据同步表资源库
 *
 * @author isacc 2019-05-17 14:07:48
 */
@SuppressWarnings("UnusedReturnValue")
public interface DataxSyncRepository {

    /**
     * 往同步表插入数据
     *
     * @param dataxSyncDTO DataxSyncDTO
     * @return com.isacc.datax.api.dto.DataxSyncDTO
     * @author isacc 2019/6/25 9:49
     */
    DataxSyncDTO insertSelectiveDTO(DataxSyncDTO dataxSyncDTO);

    /**
     * 更新同步表数据
     *
     * @param dataxSyncDTO DataxSyncDTO
     * @return com.isacc.datax.api.dto.DataxSyncDTO
     * @author isacc 2019/6/25 9:49
     */
    DataxSyncDTO updateSelectiveDTO(DataxSyncDTO dataxSyncDTO);

    /**
     * 分页条件查询同步列表
     *
     * @param dataxSyncPage page
     * @param dataxSyncDTO  DataxSyncDTO
     * @return com.baomidou.mybatisplus.core.metadata.IPage<com.isacc.datax.api.dto.DataxSyncDTO>
     * @author isacc 2019/6/25 16:23
     */
    IPage<DataxSyncDTO> pageAndSortDTO(Page<DataxSync> dataxSyncPage, DataxSyncDTO dataxSyncDTO);

    /**
     * 根据主键查询同步信息
     *
     * @param syncId 主键ID
     * @return com.isacc.datax.api.dto.DataxSyncDTO
     * @author isacc 2019/6/25 16:27
     */
    DataxSyncDTO selectByPrimaryKey(Long syncId);

    /**
     * 删除
     *
     * @param syncId 主键id
     * @return int
     * @author isacc 2019/6/25 16:34
     */
    int deleteByPrimaryKey(Long syncId);

    /**
     * 条件统计总数
     *
     * @param dataxSync DataxSync
     * @return int
     * @author isacc 2019/6/25 16:34
     */
    int selectCount(DataxSync dataxSync);
}
