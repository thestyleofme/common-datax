package com.isacc.datax.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

/**
 * <p>
 * 对API调用的返回结果封装类
 * </p>
 *
 * @author isacc 2019/04/29 0:59
 */
@Builder
@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ApiResult<T> {

	/**
	 * 返回给前台的状态码
	 */
	private Integer code;
	/**
	 * 请求是否成功
	 */
	private Boolean result;
	/**
	 * 返回给前台的信息
	 */
	private String message;
	/**
	 * 返回给前台的数据
	 */
	private T content;

}
