package com.isacc.datax.infra.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

/**
 * <p>
 * Datax Properties
 * </p>
 *
 * @author isacc 2019/05/05 17:19
 */
@Component
@Data
@Configuration
@ConfigurationProperties(prefix = DataxProperties.PROPERTY_PREFIX)
public class DataxProperties {

	public static final String PROPERTY_PREFIX = "datax";
	/**
	 * datax home
	 */
	private String home;
	/**
	 * datax server host
	 */
	private String host;
	/**
	 * datax server port
	 */
	private String port;
	/**
	 * datax server username
	 */
	private String username;
	/**
	 * datax server username
	 */
	private String password;
	/**
	 * 生成的datax json文件上传到datax服务器目录
	 */
	private String uploadDicPath;
	/**
	 * 本地生成的datax json文件在目录
	 */
	private String localDicPath;
	/**
	 * freemarker文件所在目录
	 */
	private String basePackagePath;

	private Mysql2Hive mysql2Hive;

	@SuppressWarnings("WeakerAccess")
	@Data
	public static class Mysql2Hive {
		/**
		 * 增量导入freemarker模板文件
		 */
		private String whereTemplate;

	}

}
