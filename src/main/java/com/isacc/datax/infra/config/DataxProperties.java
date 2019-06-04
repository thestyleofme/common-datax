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
    private Mysql2Mysql mysql2Mysql;
    private Mysql2Oracle mysql2Oracle;
    private String hive2HiveTemplate;
    private String hive2MysqlTemplate;
    private String hive2OracleTemplate;
    private Oracle2Hive oracle2Hive;
    private Oracle2Oracle oracle2Oracle;
    private Oracle2Mysql oracle2Mysql;

    @SuppressWarnings("WeakerAccess")
    @Data
    public static class Mysql2Hive {
        /**
         * mysql2hive的freemarker模板文件名称，使用where
         */
        private String whereTemplate;
        /**
         * mysql2hive的freemarker模板文件名称，使用querySql
         */
        private String querySqlTemplate;
    }

    @SuppressWarnings("WeakerAccess")
    @Data
    public static class Mysql2Oracle {
        /**
         * mysql2oracle的freemarker模板文件名称，使用where
         */
        private String whereTemplate;
        /**
         * mysql2oracle的freemarker模板文件名称，使用querySql
         */
        private String querySqlTemplate;
    }

    @SuppressWarnings("WeakerAccess")
    @Data
    public static class Oracle2Hive {
        /**
         * oracle2hive的freemarker模板文件名称，使用where
         */
        private String whereTemplate;
        /**
         * oracle2hive的freemarker模板文件名称，使用querySql
         */
        private String querySqlTemplate;
    }

    @SuppressWarnings("WeakerAccess")
    @Data
    public static class Mysql2Mysql {
        /**
         * mysql2mysql的freemarker模板文件名称，使用where
         */
        private String whereTemplate;
        /**
         * mysql2mysql的freemarker模板文件名称，使用querySql
         */
        private String querySqlTemplate;
    }

    @SuppressWarnings("WeakerAccess")
    @Data
    public static class Oracle2Oracle {
        /**
         * oracle2oracle的freemarker模板文件名称，使用where
         */
        private String whereTemplate;
        /**
         * oracle2oracle的freemarker模板文件名称，使用querySql
         */
        private String querySqlTemplate;
    }

    @SuppressWarnings("WeakerAccess")
    @Data
    public static class Oracle2Mysql {
        /**
         * oracle2mysql的freemarker模板文件名称，使用where
         */
        private String whereTemplate;
        /**
         * oracle2mysql的freemarker模板文件名称，使用querySql
         */
        private String querySqlTemplate;
    }

}
