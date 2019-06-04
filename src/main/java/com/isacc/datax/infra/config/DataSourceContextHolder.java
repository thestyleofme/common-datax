package com.isacc.datax.infra.config;

/**
 * description
 *
 * @author isacc 2019/06/03 22:00
 */
public class DataSourceContextHolder {

    private static final ThreadLocal<String> DATA_SOURCE_CONTEXT_HOLDER = new ThreadLocal<>();

    private DataSourceContextHolder() {
        throw new IllegalStateException("config class");
    }

    /**
     * 设置数据源
     *
     * @param dsType MultiDataSourceEnum
     * @author isacc 2019/6/3 22:02
     */
    public static void setDsType(MultiDataSourceEnum dsType) {
        DATA_SOURCE_CONTEXT_HOLDER.set(dsType.getValue());
    }

    /**
     * 取得当前数据源
     *
     * @return java.lang.String
     * @author isacc 2019/6/3 22:02
     */
    public static String getDsType() {
        return DATA_SOURCE_CONTEXT_HOLDER.get();
    }

    /**
     * 清除上下文数据
     */
    public static void clearDsType() {
        DATA_SOURCE_CONTEXT_HOLDER.remove();
    }
}
