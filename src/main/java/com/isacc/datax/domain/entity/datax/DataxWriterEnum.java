package com.isacc.datax.domain.entity.datax;

/**
 * <p>
 * DataX Writer名称枚举类
 * </p>
 *
 * @author isacc 2019/04/29 13:48
 */
public enum DataxWriterEnum {
	/**
	 * DataX Mysql Writer插件
	 */
	MYSQL_WRITER("mysqlwriter"),
	/**
	 * DataX Hdfs Writer插件
	 */
	HDFS_WRITER("hdfswriter");

	/**
	 * DataX Writer Name
	 */
	private String name;

	DataxWriterEnum(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}


}
