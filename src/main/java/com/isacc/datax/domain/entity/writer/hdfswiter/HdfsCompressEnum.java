package com.isacc.datax.domain.entity.writer.hdfswiter;

/**
 * <p>
 * Hdfs压缩类型
 * </p>
 *
 * @author HP_USER 2019/04/29 16:10
 */
public enum HdfsCompressEnum {
	/**
	 * gzip
	 */
	GZIP("gzip"),
	/**
	 * bz2
	 */
	BZ2("bz2"),
	/**
	 * zip
	 */
	ZIP("zip"),
	/**
	 * lzo
	 */
	LZO("lzo"),
	/**
	 * lzo_deflate
	 */
	LZO_DEFLATE("lzo_deflate"),
	/**
	 * hadoop上的snappy stream format
	 */
	HADOOP_SNAPPY("hadoop-snappy"),
	/**
	 * google建议的snappy stream format
	 */
	FRAMING_SNAPPY("framing-snappy"),
	/**
	 * bzip2
	 */
	BZIP2("bzip2"),
	/**
	 * NONE
	 */
	NONE("NONE"),
	/**
	 * SNAPPY（需要用户安装SnappyCodec）
	 */
	SNAPPY("SNAPPY");

	/**
	 * 压缩类型
	 */
	private String type;

	HdfsCompressEnum(String type) {
		this.type = type;
	}

	public String getType() {
		return type;
	}


}
