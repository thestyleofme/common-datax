package com.isacc.datax.domain.entity.datax;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

/**
 * <p>
 * DataX Setting
 * </p>
 *
 * @author isacc 2019/04/29 13:59
 */
@Builder
@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DataxSetting {

	private String speed;
	private String errorLimit;

}
