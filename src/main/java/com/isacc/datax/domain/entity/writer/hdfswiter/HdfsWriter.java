package com.isacc.datax.domain.entity.writer.hdfswiter;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.isacc.datax.domain.entity.reader.hdfsreader.HdfsColumn;
import com.isacc.datax.domain.entity.writer.BaseWriter;
import lombok.*;

import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;

/**
 * <p>
 * DataX Hdfs Writer插件的parameter封装
 * </p>
 *
 * @author isacc 2019/04/29 15:15
 */
@Builder
@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class HdfsWriter extends BaseWriter {

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
	 * 文件的类型，目前只支持用户配置为"text"、"orc"
	 */
	@NotBlank
	private String fileType;
	/**
	 * HdfsWriter写入时的文件名
	 */
	@NotBlank
	private String fileName;
	/**
	 * 写入数据的字段
	 */
	@NotEmpty
	private List<HdfsColumn> column;
	/**
	 * hdfswriter写入前数据清理处理模式：
	 */
	@NotBlank
	private String writeMode;
	/**
	 * hdfswriter写入时的字段分隔符
	 */
	@NotBlank
	private String fieldDelimiter;
	/**
	 * text类型文件支持压缩类型有gzip、bzip2;orc类型文件支持的压缩类型有NONE、SNAPPY（需要用户安装SnappyCodec）
	 */
	private String compress;
	/**
	 * 读取文件的编码配置
	 * 默认值：utf-8
	 */
	private String encoding;
	/**
	 * 配置与Hadoop相关的一些高级参数，比如HA的配置
	 */
	private Map<String, Object> hadoopConfig;
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

}
