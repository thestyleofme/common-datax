package com.isacc.datax.infra.constant;

/**
 * <p>
 * datax的数据同步任务类型，格式 source-target(全小写)
 * </P>
 *
 * @author isacc 2019/05/23 13:50
 */
public final class DataxHandlerTypeConstants {

    private DataxHandlerTypeConstants() {
        throw new IllegalStateException("constant class!");
    }

    public static final String MYSQL2MYSQL = "MYSQL-MYSQL";
    public static final String MYSQL2HIVE = "MYSQL-HADOOP_HIVE_2";
    public static final String MYSQL2ORACLE = "MYSQL-ORACLE";
    public static final String HIVE2HIVE = "HADOOP_HIVE_2-HADOOP_HIVE_2";
    public static final String HIVE2MYSQL = "HADOOP_HIVE_2-MYSQL";
    public static final String HIVE2ORACLE = "HADOOP_HIVE_2-ORACLE";
    public static final String ORACLE2HIVE = "ORACLE-HADOOP_HIVE_2";
    public static final String ORACLE2MYSQL = "ORACLE-MYSQL";
    public static final String ORACLE2ORACLE = "ORACLE-ORACLE";

}
