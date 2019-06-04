package com.isacc.datax.infra.config;


import javax.sql.DataSource;

import com.baomidou.dynamic.datasource.DynamicRoutingDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * <p>
 * JdbcTemplateConfig
 * </p>
 *
 * @author isacc 2019/06/04 20:52
 */
@Configuration
public class JdbcTemplateConfig {

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Bean
    public JdbcTemplate hiveJdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(((DynamicRoutingDataSource) dataSource).getDataSource("hive"));
    }
}
