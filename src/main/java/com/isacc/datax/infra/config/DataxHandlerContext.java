package com.isacc.datax.infra.config;

import java.util.Map;
import java.util.Optional;

import com.isacc.datax.app.service.DataxHandler;
import com.isacc.datax.infra.util.BeanUtil;


/**
 * <p>
 * DataxHandlerContext
 * </P>
 *
 * @author isacc 2019/05/23 9:40
 */
@SuppressWarnings({"unchecked"})
public class DataxHandlerContext {

    private Map<String, Class> handlerMap;

    public DataxHandlerContext(Map<String, Class> handlerMap) {
        this.handlerMap = handlerMap;
    }

    public DataxHandler getInstance(String type) {
        Class clazz = Optional.of(handlerMap.get(type)).orElseThrow(IllegalArgumentException::new);
        return (DataxHandler) BeanUtil.getBean(clazz);
    }
}
