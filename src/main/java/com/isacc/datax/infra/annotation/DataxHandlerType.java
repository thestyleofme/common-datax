package com.isacc.datax.infra.annotation;

import java.lang.annotation.*;

/**
 * <p>
 * 自定义注解，用于标识datax同步任务的类型
 * </P>
 *
 * @author isacc 2019/05/23 9:19
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface DataxHandlerType {
    String value();
}
