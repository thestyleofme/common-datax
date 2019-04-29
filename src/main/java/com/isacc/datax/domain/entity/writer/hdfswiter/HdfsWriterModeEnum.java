package com.isacc.datax.domain.entity.writer.hdfswiter;

/**
 * <p>
 * HdfsWriter Mode
 * </p>
 *
 * @author isacc 2019/04/28 14:25
 */
public enum HdfsWriterModeEnum {
	/**
	 * append，写入前不做任何处理，DataX hdfswriter直接使用filename写入，并保证文件名不冲突。
	 */
	APPEND("append"),
	/**
	 * nonConflict，如果目录下有fileName前缀的文件，直接报错。
	 */
	NON_CONFLICT("nonConflict");

	/**
	 * hdfs write mode
	 */
	private String writeMode;

	public String getWriteMode() {
		return writeMode;
	}

	HdfsWriterModeEnum(String writeMode) {
		this.writeMode = writeMode;
	}

}
