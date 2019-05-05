package com.isacc.datax.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;
import org.apache.http.HttpStatus;

/**
 * <p>
 * 对API调用的返回结果封装类
 * </p>
 *
 * @author isacc 2019/04/29 0:59
 */
@SuppressWarnings("unused")
@Builder
@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ApiResult<T> {

	/**
	 * 注意 这两个是共用的
	 */
	public static final ApiResult<Object> SUCCESS = new ApiResult<>(HttpStatus.SC_OK, true);
	public static final ApiResult<Object> FAILURE = new ApiResult<>(HttpStatus.SC_INTERNAL_SERVER_ERROR, false);

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

	private ApiResult(Integer code, Boolean result) {
		this.code = code;
		this.result = result;
	}

	/**
	 * 防止序列化失败，先init下或重新赋值，比如ApiResult.SUCCESS.content会序列化失败
	 *
	 * @return ApiResult<Object>
	 */
	public static ApiResult<Object> initSuccess() {
		ApiResult.SUCCESS.setContent(null);
		ApiResult.SUCCESS.setMessage(null);
		return ApiResult.SUCCESS;
	}

	public static ApiResult<Object> initFailure() {
		ApiResult.FAILURE.setContent(null);
		ApiResult.FAILURE.setMessage(null);
		return ApiResult.FAILURE;
	}

}
