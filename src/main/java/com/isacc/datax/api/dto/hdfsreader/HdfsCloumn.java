package com.isacc.datax.api.dto.hdfsreader;


import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

/**
 * <p>
 * 指定Column信息，type必须填写，index/value必须选择其一
 * </p>
 *
 * @author lei.xie03@hand-china.com 2019/04/28 15:05
 */
@Builder
@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class HdfsCloumn {

	/**
	 * index指定当前列来自于文本第几列(以0开始)
	 */
	private Integer index;
	/**
	 * type指定源数据的类型
	 */
	private String type;
	/**
	 * value指定当前类型为常量，不从源头文件读取数据，而是根据value值自动生成对应的列
	 */
	private String value;
}
