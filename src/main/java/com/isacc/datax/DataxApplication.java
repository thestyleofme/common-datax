package com.isacc.datax;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.freemarker.FreeMarkerAutoConfiguration;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;

/**
 * <p>
 * DataX启动类
 * </p>
 *
 * @author isacc 2019/04/28 17:19
 */
@SpringBootApplication(exclude = {FreeMarkerAutoConfiguration.class})
@MapperScan("com.isacc.datax.infra.mapper")
//@EnableDiscoveryClient
//@EnableFeignClients
public class DataxApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataxApplication.class, args);
    }

}
