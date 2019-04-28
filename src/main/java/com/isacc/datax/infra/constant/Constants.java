package com.isacc.datax.infra.constant;

import java.util.Locale;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * <p>
 * 常量配置
 * </p>
 *
 * @author isacc 2019/04/28 20:29
 */
public interface Constants {
	Long DEFAULT_TENANT_ID = 0L;
	String PAGE = "0";
	String SIZE = "10";
	String PAGE_FIELD_NAME = "page";
	String SIZE_FIELD_NAME = "size";
	int NEGATIVE_ONE = -1;
	int PAGE_NUM = 0;
	int PAGE_SIZE = 10;
	String FIELD_BODY = "body";
	String FIELD_CONTENT = "content";
	Locale DEFAULT_LOCALE = Locale.CHINA;
	String DEFAULT_LOCALE_STR = Locale.CHINA.toString();
	String FIELD_MSG = "message";
	String FIELD_FAILED = "failed";
	String FIELD_SUCCESS = "success";
	String FIELD_ERROR_MSG = "errorMsg";
	String DEFAULT_CHARSET = "UTF-8";
	String DEFAULT_ENV = "dev";
	ObjectMapper MAPPER = new ObjectMapper();
	String DEFAULT_CROWN_CODE = "+86";

	public interface Symbol {
		String SIGH = "!";
		String AT = "@";
		String WELL = "#";
		String DOLLAR = "$";
		String RMB = "￥";
		String SPACE = " ";
		String LB = System.getProperty("line.separator");
		String PERCENTAGE = "%";
		String AND = "&";
		String STAR = "*";
		String MIDDLE_LINE = "-";
		String LOWER_LINE = "_";
		String EQUAL = "=";
		String PLUS = "+";
		String COLON = ":";
		String SEMICOLON = ";";
		String COMMA = ",";
		String POINT = ".";
		String SLASH = "/";
		String DOUBLE_SLASH = "//";
		String BACKSLASH = "\\";
		String SINGLE_QUOTE = "\"";
		String QUESTION = "?";
		String LEFT_BIG_BRACE = "{";
		String LEFT_BRACE = "(";
		String RIGHT_BIG_BRACE = "}";
		String RIGHT_BRACE = ")";
		String LEFT_MIDDLE_BRACE = "[";
		String RIGHT_MIDDLE_BRACE = "]";
		String BACKQUOTE = "`";
	}

	public interface HeaderParam {
		String REQUEST_HEADER_PARAM_PREFIX = "param-";
	}

	public interface Digital {
		int NEGATIVE_ONE = -1;
		int ZERO = 0;
		int ONE = 1;
		int TWO = 2;
		int FOUR = 4;
		int EIGHT = 8;
		int SIXTEEN = 16;
	}

	public interface Flag {
		Integer YES = 1;
		Integer NO = 0;
	}

	public interface Pattern {
		String DATE = "yyyy-MM-dd";
		String DATETIME = "yyyy-MM-dd HH:mm:ss";
		String DATETIME_MM = "yyyy-MM-dd HH:mm";
		String DATETIME_SSS = "yyyy-MM-dd HH:mm:ss.SSS";
		String TIME = "HH:mm";
		String TIME_SS = "HH:mm:ss";
		String SYS_DATE = "yyyy/MM/dd";
		String SYS_DATETIME = "yyyy/MM/dd HH:mm:ss";
		String SYS_DATETIME_MM = "yyyy/MM/dd HH:mm";
		String SYS_DATETIME_SSS = "yyyy/MM/dd HH:mm:ss.SSS";
		String NONE_DATE = "yyyyMMdd";
		String NONE_DATETIME = "yyyyMMddHHmmss";
		String NONE_DATETIME_MM = "yyyyMMddHHmm";
		String NONE_DATETIME_SSS = "yyyyMMddHHmmssSSS";
		String CST_DATETIME = "EEE MMM dd HH:mm:ss 'CST' yyyy";
		String NONE_DECIMAL = "0";
		String ONE_DECIMAL = "0.0";
		String TWO_DECIMAL = "0.00";
		String TB_NONE_DECIMAL = "#,##0";
		String TB_ONE_DECIMAL = "#,##0.0";
		String TB_TWO_DECIMAL = "#,##0.00";
	}

}
