package com.isacc.datax.infra.config;


import javax.sql.DataSource;

import com.baomidou.dynamic.datasource.DynamicRoutingDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * <p>
 * 不知道为啥@DS("hive")不能切换数据源
 * 所以这里制定下hive数据源
 * 有知道的大神可以联系我
 * qq: 283273332
 * </p>
 *
 * @author isacc 2019/06/04 20:52
 */
@Configuration
public class JdbcTemplateConfig {

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(((DynamicRoutingDataSource) dataSource).getDataSource("hive"));
    }
}
