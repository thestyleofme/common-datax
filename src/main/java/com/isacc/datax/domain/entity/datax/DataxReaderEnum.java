package com.isacc.datax.domain.entity.datax;

/**
 * <p>
 * DataX Reader名称枚举类
 * </p>
 *
 * @author isacc 2019/04/29 13:48
 */
public enum DataxReaderEnum {
	/**
	 * DataX Mysql Reader插件
	 */
	MYSQL_READER("mysqlreader"),
	/**
	 * DataX Hdfs Reader插件
	 */
	HDFS_READER("hdfsreader");

	/**
	 * DataX Reader Name
	 */
	private String name;

	DataxReaderEnum(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}


}
