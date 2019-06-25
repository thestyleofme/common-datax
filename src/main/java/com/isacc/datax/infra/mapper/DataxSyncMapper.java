package com.isacc.datax.infra.mapper;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.isacc.datax.domain.entity.DataxSync;

/**
 * 数据同步表Mapper
 *
 * @author isacc 2019-05-17 14:07:48
 */
@DS("mysql_1")
public interface DataxSyncMapper extends BaseMapper<DataxSync> {
}
