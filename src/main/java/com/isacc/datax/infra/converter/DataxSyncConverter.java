package com.isacc.datax.infra.converter;

import java.util.Optional;

import com.isacc.datax.api.dto.DataxSyncDTO;
import com.isacc.datax.domain.entity.DataxSync;
import com.isacc.datax.infra.dataobject.DataxSyncDO;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Component;

/**
 * 数据同步表
 *
 * @author isacc 2019-05-17 14:07:48
 */
@Component
public class DataxSyncConverter implements ConvertorI<DataxSync, DataxSyncDO, DataxSyncDTO> {


    @Override
    public DataxSync dtoToEntity(DataxSyncDTO dto) {
        return Optional.ofNullable(dto).map(o -> {
            DataxSync entity = DataxSync.builder().build();
            BeanUtils.copyProperties(dto, entity);
            return entity;
        }).orElseThrow(IllegalArgumentException::new);
    }

    @Override
    public DataxSyncDTO entityToDto(DataxSync entity) {
        return Optional.ofNullable(entity).map(o -> {
            DataxSyncDTO dto = DataxSyncDTO.builder().build();
            BeanUtils.copyProperties(entity, dto);
            return dto;
        }).orElseThrow(IllegalArgumentException::new);
    }
}