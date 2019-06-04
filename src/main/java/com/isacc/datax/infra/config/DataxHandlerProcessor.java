package com.isacc.datax.infra.config;

import java.util.HashMap;
import java.util.Optional;

import com.google.common.collect.Maps;
import com.isacc.datax.infra.annotation.DataxHandlerType;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.AnnotationUtils;

/**
 * <p>
 * <span>@DataxHandlerType注解扫描</span>
 * </P>
 *
 * @author isacc 2019/05/23 23:19
 */
@Configuration
public class DataxHandlerProcessor implements BeanPostProcessor {

    private HashMap<String, Class> handlerMap = Maps.newHashMapWithExpectedSize(16);

    @SuppressWarnings("NullableProblems")
    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) {
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        DataxHandlerType annotation = AnnotationUtils.findAnnotation(bean.getClass(), DataxHandlerType.class);
        Optional.ofNullable(annotation).ifPresent(o -> handlerMap.put(o.value(), bean.getClass()));
        return bean;
    }

    @Bean
    public DataxHandlerContext dataxHandlerContext() {
        return new DataxHandlerContext(handlerMap);
    }
}
