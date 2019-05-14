package com.isacc.datax.infra.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

/**
 * <p>
 * description
 * </P>
 *
 * @author isacc 2019/05/13 22:11
 */
@Component
@Data
@Configuration
@ConfigurationProperties(prefix = AzkabanProperties.PROPERTY_PREFIX)
public class AzkabanProperties {

    public static final String PROPERTY_PREFIX = "azkaban";

    /**
     * 主机名
     */
    private String host;
    /**
     * 用户名
     */
    private String username;
    /**
     * 密码
     */
    private String password;
    /**
     * 本地存储路径
     */
    private String localDicPath;
    /**
     * template名称
     */
    private String templateName;
    /**
     * dataxJob.job的path
     */
    private String dataxJob;
    /**
     * dataxParams.properties
     */
    private String dataxProperties;
}
