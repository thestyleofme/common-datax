package com.isacc.datax.domain.entity.reader.oraclereader;

import java.util.List;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.isacc.datax.domain.entity.reader.BaseReader;
import com.isacc.datax.domain.entity.reader.mysqlreader.ReaderConnection;
import lombok.*;

/**
 * DataX oraclereader封装
 *
 * @author isacc 2019/05/28 10:20
 */
@Builder
@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OracleReader extends BaseReader {

    /**
     * 数据库连接用户名
     */
    @NotBlank
    private String username;
    /**
     * 数据库连接密码
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
     * 使用splitPk代表的字段进行数据分片，仅支持整形数据切分，不支持浮点、字符串、日期等其他类型
     */
    private String splitPk;
    /**
     * 筛选条件，Reader根据指定的column、table、where条件拼接SQL，并根据这个SQL进行数据抽取
     */
    private String where;
    /**
     * oracle连接信息
     */
    @NotEmpty
    private List<ReaderConnection> connection;
    /**
     * 插件和数据库服务器端每次批量数据获取条数,该值决定了DataX和服务器端的网络交互次数，能够较大的提升数据抽取性能
     * 注意，该值过大(>2048)可能造成DataX进程OOM
     * 默认值：1024
     */
    private Long fetchSize;
    /**
     * 控制写入数据的时间格式，时区等的配置
     */
    private List<String> session;

}
