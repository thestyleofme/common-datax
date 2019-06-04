package com.isacc.datax.domain.entity.writer.oraclewriter;

import java.util.List;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.isacc.datax.domain.entity.writer.BaseWriter;
import com.isacc.datax.domain.entity.writer.mysqlwriter.WriterConnection;
import lombok.*;

/**
 * DataX oraclewriter封装
 *
 * @author isacc 2019/05/28 10:55
 */
@Builder
@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OracleWriter extends BaseWriter {

    /**
     * 目的数据库的用户名
     */
    @NotBlank
    private String username;
    /**
     * 目的数据库的密码
     */
    @NotBlank
    private String password;
    /**
     * 所配置的表中需要同步的列名集合
     * 全选时值为：['*']
     */
    @NotEmpty
    private List<String> column;
    /**
     * 写入数据到目的表前，会先执行这里的标准语句
     */
    private List<String> preSql;
    /**
     * 写入数据到目的表后，会执行这里的标准语句
     */
    private List<String> postSql;
    /**
     * 一次性批量提交的记录数大小，该值可以极大减少DataX与oracle的网络交互次数，并提升整体吞吐量
     * 但是该值设置过大可能会造成DataX运行进程OOM情况。
     * 默认值：1024
     */
    private Long batchSize;
    /**
     * 设置oracle连接时的session信息
     * 如： "alter session set nls_date_format = 'dd.mm.yyyy hh24:mi:ss';"
     */
    private List<String> session;
    /**
     * Oracle连接信息
     */
    @NotNull
    private List<WriterConnection> connection;

}
