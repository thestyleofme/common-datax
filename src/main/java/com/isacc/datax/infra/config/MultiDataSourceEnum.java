package com.isacc.datax.infra.config;

/**
 * description
 *
 * @author isacc 2019/06/03 21:56
 */
public enum MultiDataSourceEnum {

    /**
     * hive
     */
    HIVE("hive"),
    /**
     * mysql_hivemeta
     */
    MYSQL_HIVEMETA("mysql_hivemeta"),
    /**
     * mysql_1
     */
    MYSQL_1("mysql_1"),
    /**
     * mysql_2
     */
    MYSQL_2("mysql_2");

    private String value;

    MultiDataSourceEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

}
