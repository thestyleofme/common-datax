package com.isacc.datax.domain.entity.reader;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

/**
 * <p>
 * DataX Reader
 * </p>
 *
 * @author isacc 2019/04/29 14:03
 */
@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BaseReader {
	/**
	 * DataX reader插件名称
	 */
	protected String name;
}
