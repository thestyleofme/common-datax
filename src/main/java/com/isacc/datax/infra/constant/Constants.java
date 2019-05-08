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
@SuppressWarnings("unused")
public final class Constants {
    private Constants() {
        throw new IllegalStateException();
    }

    public static final Long DEFAULT_TENANT_ID = 0L;
    public static final String PAGE = "0";
    public static final String SIZE = "10";
    public static final String PAGE_FIELD_NAME = "page";
    public static final String SIZE_FIELD_NAME = "size";
    public static final int NEGATIVE_ONE = -1;
    public static final int PAGE_NUM = 0;
    public static final int PAGE_SIZE = 10;
    public static final String FIELD_BODY = "body";
    public static final String FIELD_CONTENT = "content";
    public static final Locale DEFAULT_LOCALE = Locale.CHINA;
    public static final String DEFAULT_LOCALE_STR = Locale.CHINA.toString();
    public static final String FIELD_MSG = "message";
    public static final String FIELD_FAILED = "failed";
    public static final String FIELD_SUCCESS = "success";
    public static final String FIELD_ERROR_MSG = "errorMsg";
    public static final String DEFAULT_CHARSET = "UTF-8";
    public static final String DEFAULT_ENV = "dev";
    public static final ObjectMapper MAPPER = new ObjectMapper();
    public static final String DEFAULT_CROWN_CODE = "+86";
    public static final String DB_IS_NOT_EXIST = "DB_IS_NOT_EXIST";
    public static final String TBL_IS_NOT_EXIST = "TBL_IS_NOT_EXIST";

    public static final class Symbol {
        private Symbol() {
            throw new IllegalStateException();
        }

        public static final String SIGH = "!";
        public static final String AT = "@";
        public static final String WELL = "#";
        public static final String DOLLAR = "$";
        public static final String RMB = "￥";
        public static final String SPACE = " ";
        public static final String LB = System.getProperty("line.separator");
        public static final String PERCENTAGE = "%";
        public static final String AND = "&";
        public static final String STAR = "*";
        public static final String MIDDLE_LINE = "-";
        public static final String LOWER_LINE = "_";
        public static final String EQUAL = "=";
        public static final String PLUS = "+";
        public static final String COLON = ":";
        public static final String SEMICOLON = ";";
        public static final String COMMA = ",";
        public static final String POINT = ".";
        public static final String SLASH = "/";
        public static final String DOUBLE_SLASH = "//";
        public static final String BACKSLASH = "\\";
        public static final String SINGLE_QUOTE = "\"";
        public static final String QUESTION = "?";
        public static final String LEFT_BIG_BRACE = "{";
        public static final String LEFT_BRACE = "(";
        public static final String RIGHT_BIG_BRACE = "}";
        public static final String RIGHT_BRACE = ")";
        public static final String LEFT_MIDDLE_BRACE = "[";
        public static final String RIGHT_MIDDLE_BRACE = "]";
        public static final String BACKQUOTE = "`";
    }

    public static final class HeaderParam {
        private HeaderParam() {
            throw new IllegalStateException();
        }

        public static final String REQUEST_HEADER_PARAM_PREFIX = "param-";
    }

    public static final class Digital {
        private Digital() {
            throw new IllegalStateException();
        }

        public static final int NEGATIVE_ONE = -1;
        public static final int ZERO = 0;
        public static final int ONE = 1;
        public static final int TWO = 2;
        public static final int FOUR = 4;
        public static final int EIGHT = 8;
        public static final int SIXTEEN = 16;
    }

    public static final class Flag {
        private Flag() {
            throw new IllegalStateException();
        }

        public static final Integer YES = 1;
        public static final Integer NO = 0;
    }

    public static final class Pattern {
        private Pattern() {
            throw new IllegalStateException();
        }

        public static final String DATE = "yyyy-MM-dd";
        public static final String DATETIME = "yyyy-MM-dd HH:mm:ss";
        public static final String DATETIME_MM = "yyyy-MM-dd HH:mm";
        public static final String DATETIME_SSS = "yyyy-MM-dd HH:mm:ss.SSS";
        public static final String TIME = "HH:mm";
        public static final String TIME_SS = "HH:mm:ss";
        public static final String SYS_DATE = "yyyy/MM/dd";
        public static final String SYS_DATETIME = "yyyy/MM/dd HH:mm:ss";
        public static final String SYS_DATETIME_MM = "yyyy/MM/dd HH:mm";
        public static final String SYS_DATETIME_SSS = "yyyy/MM/dd HH:mm:ss.SSS";
        public static final String NONE_DATE = "yyyyMMdd";
        public static final String NONE_DATETIME = "yyyyMMddHHmmss";
        public static final String NONE_DATETIME_MM = "yyyyMMddHHmm";
        public static final String NONE_DATETIME_SSS = "yyyyMMddHHmmssSSS";
        public static final String CST_DATETIME = "EEE MMM dd HH:mm:ss 'CST' yyyy";
        public static final String NONE_DECIMAL = "0";
        public static final String ONE_DECIMAL = "0.0";
        public static final String TWO_DECIMAL = "0.00";
        public static final String TB_NONE_DECIMAL = "#,##0";
        public static final String TB_ONE_DECIMAL = "#,##0.0";
        public static final String TB_TWO_DECIMAL = "#,##0.00";
    }

}
