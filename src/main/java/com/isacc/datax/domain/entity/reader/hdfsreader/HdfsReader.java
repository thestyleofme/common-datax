package com.isacc.datax.domain.entity.reader.hdfsreader;

import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

/**
 * <p>
 * hdfsreader插件的参数封装
 * </p>
 *
 * @author isacc 2019/04/28 14:43
 */
@Builder
@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class HdfsReader {

	/**
	 * 要读取的文件路径，如果要读取多个文件，可以使用正则表达式"*"，只支持"*"和"?"作为文件通配符
	 */
	@NotBlank
	private String path;
	/**
	 * Hadoop hdfs文件系统namenode节点地址
	 */
	@NotBlank
	private String defaultFS;
	/**
	 * 文件的类型，目前只支持用户配置为"text"、"orc"、"rc"、"seq"、"csv"
	 */
	@NotBlank
	private String fileType;
	/**
	 * 读取字段列表
	 */
	@NotEmpty
	private List<HdfsColumn> column;
	/**
	 * 读取的字段分隔符，默认为','
	 * HdfsReader在读取orcfile时，用户无需指定字段分割符
	 */
	private String fieldDelimiter;
	/**
	 * 读取文件的编码配置
	 * 默认值：utf-8
	 */
	private String encoding;
	/**
	 * 文本文件中无法使用标准字符串定义null(空指针)，DataX提供nullFormat定义哪些字符串可以表示为null
	 * 例如配置，nullFormat:"\N"，那么如果源头数据是"\N"，DataX视作null字段
	 */
	private String nullFormat;
	/**
	 * 是否有Kerberos认证，默认false
	 * 若配置true，则配置项kerberosKeytabFilePath，kerberosPrincipal为必填
	 */
	private Boolean haveKerberos;
	/**
	 * Kerberos认证 keytab文件路径，绝对路径
	 */
	private String kerberosKeytabFilePath;
	/**
	 * Kerberos认证Principal名，如xxxx/hadoopclient@xxx.xxx
	 */
	private String kerberosPrincipal;
	/**
	 * 当fileType（文件类型）为csv下的文件压缩方式，目前仅支持 gzip、bz2、zip、lzo、lzo_deflate、hadoop-snappy、framing-snappy压缩
	 * orc文件类型下无需填写
	 */
	private String compress;
	/**
	 * 配置与Hadoop相关的一些高级参数，比如HA的配置
	 */
	private Map<String, Object> hadoopConfig;
	/**
	 * 读取CSV类型文件参数配置
	 */
	private Map<String, Object> csvReaderConfig;

}
