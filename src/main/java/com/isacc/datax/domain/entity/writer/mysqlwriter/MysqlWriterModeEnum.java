package com.isacc.datax.domain.entity.writer.mysqlwriter;

/**
 * <p>
 * MysqlWriter Mode
 * </p>
 *
 * @author isacc 2019/04/28 14:25
 */
public enum MysqlWriterModeEnum {
	/**
	 * insert into
	 */
	INSERT("insert"),
	/**
	 * replace into
	 */
	REPLACE("replace"),
	/**
	 * on duplicate key update
	 */
	UPDATE("replace");

	/**
	 * mysql write mode
	 */
	private String writeMode;

	public String getWriteMode() {
		return writeMode;
	}

	MysqlWriterModeEnum(String writeMode) {
		this.writeMode = writeMode;
	}

}
