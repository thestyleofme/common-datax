package com.isacc.datax.infra.constant;

/**
 * <p>
 * Datax各种reader以及writer的参数常量类
 * </P>
 *
 * @author isacc 2019/05/22 13:50
 */
public final class DataxParameterConstants {

    private DataxParameterConstants() {
        throw new IllegalStateException("constant class!");
    }

    public static final String SETTING = "setting";
    /**
     * mysql
     */
    public static final String MYSQL_READER_USERNAME = "mysqlreaderUsername";
    public static final String MYSQL_READER_PASSWORD = "mysqlreaderPassword";
    public static final String MYSQL_READER_CONNECTION = "mysqlreaderConnection";
    public static final String MYSQL_READER_SPLIT_PK = "mysqlreaderSplitPk";
    public static final String MYSQL_READER_WHERE = "mysqlreaderWhere";
    public static final String MYSQL_READER_COLUMN = "mysqlreaderColumn";
    public static final String MYSQL_WRITE_MODE = "writeMode";
    public static final String MYSQL_WRITER_USERNAME = "mysqlwriterUsername";
    public static final String MYSQL_WRITER_PASSWORD = "mysqlwriterPassword";
    public static final String MYSQL_WRITER_COLUMN = "mysqlwriterColumn";
    public static final String MYSQL_WRITER_BATCH_SIZE = "mysqlwriterBatchSize";
    public static final String MYSQL_WRITER_SESSION = "mysqlwriterSession";
    public static final String MYSQL_WRITER_PRE_SQL = "mysqlwriterPreSql";
    public static final String MYSQL_WRITER_CONNECTION = "mysqlwriterConnection";
    public static final String MYSQL_WRITER_POST_SQL = "mysqlwriterPostSql";

    /**
     * hdfs
     */
    public static final String HDFS_READER_COLUMN = "hdfsreaderColumn";
    public static final String HDFS_READER_DEFAULT_FS = "hdfsreaderDefaultFS";
    public static final String HDFS_READER_FILE_TYPE = "hdfsreaderFileType";
    public static final String HDFS_READER_PATH = "hdfsreaderPath";
    public static final String HDFS_READER_FIELD_DELIMITER = "hdfsreaderFieldDelimiter";
    public static final String HDFS_READER_COMPRESS = "hdfsreaderCompress";
    public static final String HDFS_READER_NULL_FORMAT = "hdfsreaderNullFormat";
    public static final String HDFS_READER_HADOOP_CONFIG = "hdfsreaderHadoopConfig";
    public static final String HDFS_READER_HAVE_KERBEROS = "hdfsreaderHaveKerberos";
    public static final String HDFS_READER_KERBEROS_KEYTAB_FILE_PATH = "hdfsreaderKerberosKeytabFilePath";
    public static final String HDFS_READER_KERBEROS_PRINCIPAL = "hdfsreaderKerberosPrincipal";
    public static final String HDFS_READER_CSV_READER_CONFIG = "hdfsreaderCsvReaderConfig";
    public static final String HDFS_WRITER_COLUMN = "hdfswriterColumn";
    public static final String HDFS_WRITER_MODE = "writeMode";
    public static final String HDFS_WRITER_DEFAULT_FS = "hdfswriterDefaultFS";
    public static final String HDFS_WRITER_FILE_TYPE = "hdfswriterFileType";
    public static final String HDFS_WRITER_PATH = "hdfswriterPath";
    public static final String HDFS_WRITER_FILE_NAME = "hdfswriterFileName";
    public static final String HDFS_WRITER_FIELD_DELIMITER = "hdfswriterFieldDelimiter";
    public static final String HDFS_WRITER_COMPRESS = "hdfswriterCompress";
    public static final String HDFS_WRITER_HADOOP_CONFIG = "hdfswriterHadoopConfig";
    public static final String HDFS_WRITER_HAVE_KERBEROS = "hdfswriterHaveKerberos";
    public static final String HDFS_WRITER_KERBEROS_KEYTAB_FILE_PATH = "hdfswriterKerberosKeytabFilePath";
    public static final String HDFS_WRITER_KERBEROS_PRINCIPAL = "hdfswriterKerberosPrincipal";

    /**
     * oracle
     */
    public static final String ORACLE_READER_USERNAME = "oraclereaderUsername";
    public static final String ORACLE_READER_PASSWORD = "oraclereaderPassword";
    public static final String ORACLE_READER_COLUMN = "oraclereaderColumn";
    public static final String ORACLE_READER_SPLIT_PK = "oraclereaderSplitPk";
    public static final String ORACLE_READER_WHERE = "oraclereaderWhere";
    public static final String ORACLE_READER_CONNECTION = "oraclereaderConnection";
    public static final String ORACLE_READER_FETCH_SIZE = "oraclereaderFetchSize";
    public static final String ORACLE_READER_SESSION = "oraclereaderSession";
    public static final String ORACLE_WRITER_USERNAME = "oraclewriterUsername";
    public static final String ORACLE_WRITER_PASSWORD = "oraclewriterPassword";
    public static final String ORACLE_WRITER_COLUMN = "oraclewriterColumn";
    public static final String ORACLE_WRITER_PRE_SQL = "oraclewriterPreSql";
    public static final String ORACLE_WRITER_POST_SQL = "oraclewriterPostSql";
    public static final String ORACLE_WRITER_BATCH_SIZE = "oraclewriterBatchSize";
    public static final String ORACLE_WRITER_SESSION = "oraclewriterSession";
    public static final String ORACLE_WRITER_CONNECTION = "oraclewriterConnection";


}
