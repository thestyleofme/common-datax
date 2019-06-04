package com.isacc.datax.infra.config;

import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

/**
 * description
 *
 * @author isacc 2019/06/03 22:05
 */
public class DynamicDataSource extends AbstractRoutingDataSource {

    @Override
    protected Object determineCurrentLookupKey() {
        return DataSourceContextHolder.getDsType();
    }

}
