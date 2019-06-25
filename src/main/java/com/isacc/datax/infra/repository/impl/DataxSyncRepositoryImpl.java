package com.isacc.datax.infra.repository.impl;


import java.util.ArrayList;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.isacc.datax.api.dto.DataxSyncDTO;
import com.isacc.datax.domain.entity.DataxSync;
import com.isacc.datax.domain.repository.DataxSyncRepository;
import com.isacc.datax.infra.converter.DataxSyncConverter;
import com.isacc.datax.infra.mapper.DataxSyncMapper;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Component;

/**
 * 数据同步表 资源库实现
 *
 * @author isacc 2019-05-17 14:07:48
 */
@Component
public class DataxSyncRepositoryImpl implements DataxSyncRepository {

    private final DataxSyncMapper dataxSyncMapper;
    private final DataxSyncConverter dataxSyncConverter;

    public DataxSyncRepositoryImpl(DataxSyncMapper dataxSyncMapper, DataxSyncConverter dataxSyncConverter) {
        this.dataxSyncMapper = dataxSyncMapper;
        this.dataxSyncConverter = dataxSyncConverter;
    }

    @Override
    public DataxSyncDTO insertSelectiveDTO(DataxSyncDTO dataxSyncDTO) {
        final DataxSync dataxSync = dataxSyncConverter.dtoToEntity(dataxSyncDTO);
        dataxSyncMapper.insert(dataxSync);
        return dataxSyncConverter.entityToDto(dataxSync);
    }

    @Override
    public DataxSyncDTO updateSelectiveDTO(DataxSyncDTO dataxSyncDTO) {
        final DataxSync dataxSync = dataxSyncConverter.dtoToEntity(dataxSyncDTO);
        dataxSyncMapper.updateById(dataxSync);
        return dataxSyncDTO;
    }

    @Override
    public IPage<DataxSyncDTO> pageAndSortDTO(Page<DataxSync> dataxSyncPage, DataxSyncDTO dataxSyncDTO) {
        final QueryWrapper<DataxSync> queryWrapper = this.commonQueryWrapper(dataxSyncDTO);
        final IPage<DataxSync> entityPage = dataxSyncMapper.selectPage(dataxSyncPage, queryWrapper);
        final ArrayList<DataxSyncDTO> dtoList = new ArrayList<>();
        entityPage.getRecords().forEach(entity -> dtoList.add(dataxSyncConverter.entityToDto(entity)));
        final Page<DataxSyncDTO> dtoPage = new Page<>();
        BeanUtils.copyProperties(entityPage, dtoPage);
        dtoPage.setRecords(dtoList);
        return dtoPage;
    }

    @Override
    public DataxSyncDTO selectByPrimaryKey(Long syncId) {
        final DataxSync dataxSync = dataxSyncMapper.selectById(syncId);
        return dataxSyncConverter.entityToDto(dataxSync);
    }

    @Override
    public int deleteByPrimaryKey(Long syncId) {
        return dataxSyncMapper.deleteById(syncId);
    }

    private QueryWrapper<DataxSync> commonQueryWrapper(DataxSyncDTO dataxSyncDTO) {
        final QueryWrapper<DataxSync> queryWrapper = new QueryWrapper<>();
        queryWrapper.or().like("SYNC_NAME", dataxSyncDTO.getSyncName());
        queryWrapper.or().like("JSON_FILE_NAME", dataxSyncDTO.getJsonFileName());
        queryWrapper.or().eq("TENANT_ID", dataxSyncDTO.getTenantId());
        queryWrapper.or().eq("SOURCE_DATASOURCE_TYPE", dataxSyncDTO.getSourceDatasourceType());
        queryWrapper.or().eq("WRITE_DATASOURCE_TYPE", dataxSyncDTO.getWriteDatasourceType());
        return queryWrapper;
    }

    private QueryWrapper<DataxSync> checkQueryWrapper(DataxSyncDTO dataxSyncDTO) {
        final QueryWrapper<DataxSync> queryWrapper = new QueryWrapper<>();
        queryWrapper.or().eq("SYNC_NAME", dataxSyncDTO.getSyncName());
        queryWrapper.or().eq("JSON_FILE_NAME", dataxSyncDTO.getJsonFileName());
        return queryWrapper;
    }

    @Override
    public int selectCount(DataxSync dataxSync) {
        final QueryWrapper<DataxSync> queryWrapper = this.checkQueryWrapper(dataxSyncConverter.entityToDto(dataxSync));
        return dataxSyncMapper.selectCount(queryWrapper);
    }
}
