package com.isacc.datax.domain.entity.reader.hdfsreader;


/**
 * <p>
 * hdfs file type
 * </p>
 *
 * @author isacc 2019/04/28 14:59
 */
public enum HdfsFileTypeEnum {
	/**
	 * textfile文件格式
	 */
	TEXT("TEXTFILE"),
	/**
	 * orcfile文件格式
	 */
	ORC("ORC"),
	/**
	 * rcfile文件格式
	 */
	RC("RCFILE"),
	/**
	 * sequence file文件格式
	 */
	SEQ("SEQUENCEFILE"),
	/**
	 * 普通hdfs文件格式（逻辑二维表）
	 */
	CSV("CSV");

	/**
	 * hdfs file type
	 */
	private String fileType;

	HdfsFileTypeEnum(String fileType) {
		this.fileType = fileType;
	}

	public String getFileType() {
		return fileType;
	}

}
