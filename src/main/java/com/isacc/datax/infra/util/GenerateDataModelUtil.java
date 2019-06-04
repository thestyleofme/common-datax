package com.isacc.datax.infra.util;

import java.util.Map;

import com.isacc.datax.domain.entity.reader.hdfsreader.HdfsReader;
import com.isacc.datax.domain.entity.reader.mysqlreader.MysqlReader;
import com.isacc.datax.domain.entity.reader.oraclereader.OracleReader;
import com.isacc.datax.domain.entity.writer.hdfswiter.HdfsWriter;
import com.isacc.datax.domain.entity.writer.mysqlwriter.MysqlWriter;
import com.isacc.datax.domain.entity.writer.oraclewriter.OracleWriter;
import com.isacc.datax.infra.constant.DataxParameterConstants;


/**
 * <p>
 * description
 * </P>
 *
 * @author isacc 2019/05/22 16:51
 */
public class GenerateDataModelUtil {

    private GenerateDataModelUtil() {
        throw new IllegalStateException("Utility class");
    }

    public static void commonMysqlReader(Map<String, Object> root, MysqlReader mysqlReader) {
        root.put(DataxParameterConstants.MYSQL_READER_USERNAME, mysqlReader.getUsername());
        root.put(DataxParameterConstants.MYSQL_READER_PASSWORD, mysqlReader.getPassword());
        root.put(DataxParameterConstants.MYSQL_READER_CONNECTION, mysqlReader.getConnection());
        root.put(DataxParameterConstants.MYSQL_READER_SPLIT_PK, mysqlReader.getSplitPk());
    }

    public static void commonOracleReader(Map<String, Object> root, OracleReader oracleReader) {
        root.put(DataxParameterConstants.ORACLE_READER_USERNAME, oracleReader.getUsername());
        root.put(DataxParameterConstants.ORACLE_READER_PASSWORD, oracleReader.getPassword());
        root.put(DataxParameterConstants.ORACLE_READER_CONNECTION, oracleReader.getConnection());
        root.put(DataxParameterConstants.ORACLE_READER_SPLIT_PK, oracleReader.getSplitPk());
        root.put(DataxParameterConstants.ORACLE_READER_FETCH_SIZE, oracleReader.getFetchSize());
        root.put(DataxParameterConstants.ORACLE_READER_SESSION, oracleReader.getSession());
        root.put(DataxParameterConstants.ORACLE_READER_COLUMN, oracleReader.getColumn());
    }

    public static Map<String, Object> commonMysqlWriter(Map<String, Object> root, MysqlWriter mysqlWriter) {
        root.put(DataxParameterConstants.MYSQL_WRITER_USERNAME, mysqlWriter.getUsername());
        root.put(DataxParameterConstants.MYSQL_WRITER_PASSWORD, mysqlWriter.getPassword());
        root.put(DataxParameterConstants.MYSQL_WRITER_CONNECTION, mysqlWriter.getConnection());
        root.put(DataxParameterConstants.MYSQL_WRITER_COLUMN, mysqlWriter.getColumn());
        root.put(DataxParameterConstants.MYSQL_WRITER_SESSION, mysqlWriter.getSession());
        root.put(DataxParameterConstants.MYSQL_WRITER_PRE_SQL, mysqlWriter.getPreSql());
        root.put(DataxParameterConstants.MYSQL_WRITER_POST_SQL, mysqlWriter.getPostSql());
        root.put(DataxParameterConstants.MYSQL_WRITER_BATCH_SIZE, mysqlWriter.getBatchSize());
        return root;
    }

    public static Map<String, Object> commonOracleWriter(Map<String, Object> root, OracleWriter oracleWriter) {
        root.put(DataxParameterConstants.ORACLE_WRITER_USERNAME, oracleWriter.getUsername());
        root.put(DataxParameterConstants.ORACLE_WRITER_PASSWORD, oracleWriter.getPassword());
        root.put(DataxParameterConstants.ORACLE_WRITER_CONNECTION, oracleWriter.getConnection());
        root.put(DataxParameterConstants.ORACLE_WRITER_COLUMN, oracleWriter.getColumn());
        root.put(DataxParameterConstants.ORACLE_WRITER_SESSION, oracleWriter.getSession());
        root.put(DataxParameterConstants.ORACLE_WRITER_PRE_SQL, oracleWriter.getPreSql());
        root.put(DataxParameterConstants.ORACLE_WRITER_POST_SQL, oracleWriter.getPostSql());
        root.put(DataxParameterConstants.ORACLE_WRITER_BATCH_SIZE, oracleWriter.getBatchSize());
        return root;
    }

    public static void commonHdfsReader(Map<String, Object> root, HdfsReader hdfsReader) {
        root.put(DataxParameterConstants.HDFS_READER_DEFAULT_FS, hdfsReader.getDefaultFS());
        root.put(DataxParameterConstants.HDFS_READER_PATH, hdfsReader.getPath());
        root.put(DataxParameterConstants.HDFS_READER_FILE_TYPE, hdfsReader.getFileType());
        root.put(DataxParameterConstants.HDFS_READER_COLUMN, hdfsReader.getColumn());
        root.put(DataxParameterConstants.HDFS_READER_FIELD_DELIMITER, hdfsReader.getFieldDelimiter());
        root.put(DataxParameterConstants.HDFS_READER_NULL_FORMAT, hdfsReader.getNullFormat());
        root.put(DataxParameterConstants.HDFS_READER_HAVE_KERBEROS, hdfsReader.getHaveKerberos());
        root.put(DataxParameterConstants.HDFS_READER_KERBEROS_KEYTAB_FILE_PATH, hdfsReader.getKerberosKeytabFilePath());
        root.put(DataxParameterConstants.HDFS_READER_KERBEROS_PRINCIPAL, hdfsReader.getKerberosPrincipal());
        root.put(DataxParameterConstants.HDFS_READER_COMPRESS, hdfsReader.getCompress());
        root.put(DataxParameterConstants.HDFS_READER_HADOOP_CONFIG, hdfsReader.getHadoopConfig());
        root.put(DataxParameterConstants.HDFS_READER_CSV_READER_CONFIG, hdfsReader.getCsvReaderConfig());
    }

    public static Map<String, Object> commonHdfsWriter(Map<String, Object> root, HdfsWriter hdfsWriter) {
        root.put(DataxParameterConstants.HDFS_WRITER_DEFAULT_FS, hdfsWriter.getDefaultFS());
        root.put(DataxParameterConstants.HDFS_WRITER_FILE_TYPE, hdfsWriter.getFileType());
        root.put(DataxParameterConstants.HDFS_WRITER_FILE_NAME, hdfsWriter.getFileName());
        root.put(DataxParameterConstants.HDFS_WRITER_PATH, hdfsWriter.getPath());
        root.put(DataxParameterConstants.HDFS_WRITER_COLUMN, hdfsWriter.getColumn());
        root.put(DataxParameterConstants.HDFS_WRITER_FIELD_DELIMITER, hdfsWriter.getFieldDelimiter());
        root.put(DataxParameterConstants.HDFS_WRITER_COMPRESS, hdfsWriter.getCompress());
        root.put(DataxParameterConstants.HDFS_WRITER_HADOOP_CONFIG, hdfsWriter.getHadoopConfig());
        root.put(DataxParameterConstants.HDFS_WRITER_HAVE_KERBEROS, hdfsWriter.getHaveKerberos());
        root.put(DataxParameterConstants.HDFS_WRITER_KERBEROS_KEYTAB_FILE_PATH, hdfsWriter.getKerberosKeytabFilePath());
        root.put(DataxParameterConstants.HDFS_WRITER_KERBEROS_PRINCIPAL, hdfsWriter.getKerberosPrincipal());
        return root;
    }

}
