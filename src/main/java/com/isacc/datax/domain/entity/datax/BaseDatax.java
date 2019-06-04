package com.isacc.datax.domain.entity.datax;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * <p>
 * Base DataX
 * </p>
 *
 * @author isacc 2019/04/29 14:03
 */
@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BaseDatax {

    /**
     * DataX Setting
     */
    private DataxSetting setting;

}
