package com.isacc.datax.infra.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

/**
 * <p>
 * description
 * </P>
 *
 * @author isacc 2019/05/13 22:00
 */
@Configuration
public class RestTemplateConfig {

    @Bean
    public RestTemplate cusRestTemplate(ClientHttpRequestFactory customSimpleClientHttpRequestFactory) {
        return new RestTemplate(customSimpleClientHttpRequestFactory);
    }

    @Bean
    public ClientHttpRequestFactory customSimpleClientHttpRequestFactory() {
        CustomSimpleClientHttpRequestFactory factory = new CustomSimpleClientHttpRequestFactory();
        factory.setReadTimeout(5000);
        factory.setConnectTimeout(15000);
        return factory;
    }


}
