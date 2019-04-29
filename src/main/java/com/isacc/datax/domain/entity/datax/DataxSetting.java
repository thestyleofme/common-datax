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
@SuppressWarnings("WeakerAccess")
@Builder
@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DataxSetting {

	private DataxSpeed speed;
	private ErrorLimit errorLimit;

	@Builder
	@Data
	@EqualsAndHashCode(callSuper = false)
	@NoArgsConstructor
	@AllArgsConstructor
	@JsonInclude(JsonInclude.Include.NON_NULL)
	public static class ErrorLimit{

		/**
		 * record
		 */
		private String record;
		/**
		 * percentage
		 */
		private String percentage;
	}

	@Builder
	@Data
	@EqualsAndHashCode(callSuper = false)
	@NoArgsConstructor
	@AllArgsConstructor
	@JsonInclude(JsonInclude.Include.NON_NULL)
	public static class DataxSpeed {
		/**
		 * record
		 */
		private String record;
		/**
		 * channel
		 */
		private String channel;

	}

}


