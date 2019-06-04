package com.isacc.datax.domain.entity;

import java.util.Date;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * 数据同步表
 *
 * @author isacc 2019-05-17 14:07:48
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@ApiModel("数据同步表")
public class DataxSync {

    public static final String FIELD_SYNC_ID = "syncId";
    public static final String FIELD_SYNC_NAME = "syncName";
    public static final String FIELD_SYNC_DESCRIPTION = "syncDescription";
    public static final String FIELD_SOURCE_DATASOURCE_TYPE = "sourceDatasourceType";
    public static final String FIELD_SOURCE_DATASOURCE_ID = "sourceDatasourceId";
    public static final String FIELD_WRITE_DATASOURCE_TYPE = "writeDatasourceType";
    public static final String FIELD_WRITE_DATASOURCE_ID = "writeDatasourceId";
    public static final String FIELD_JSON_FILE_NAME ="jsonFileName";
    public static final String FIELD_SETTING_INFO = "settingInfo";
    public static final String FIELD_TENANT_ID = "tenantId";
    public static final String FIELD_OBJECT_VERSION_NUMBER = "objectVersionNumber";
    public static final String FIELD_CREATION_DATE = "creationDate";
    public static final String FIELD_CREATED_BY = "createdBy";
    public static final String FIELD_LAST_UPDATED_BY = "lastUpdatedBy";
    public static final String FIELD_LAST_UPDATE_DATE = "lastUpdateDate";

    //
    // 业务方法(按public protected private顺序排列)
    // ------------------------------------------------------------------------------

    //
    // 数据库字段
    // ------------------------------------------------------------------------------

    @ApiModelProperty("表ID，主键，供其他表做外键")
    @TableId(type = IdType.AUTO)
    private Long syncId;
    @ApiModelProperty(value = "同步名称", required = true)
    @NotBlank
    private String syncName;
    @ApiModelProperty(value = "同步描述", required = true)
    @NotBlank
    private String syncDescription;
    @ApiModelProperty(value = "来源数据源类型，快码：HDSP.DATASOURCE_TYPE", required = true)
    @NotBlank
    private String sourceDatasourceType;
    @ApiModelProperty(value = "来源数据源ID,关联HDSP_CORE_DATASOURCE.DATASOURCE_ID", required = true)
    @NotNull
    private Long sourceDatasourceId;
    @ApiModelProperty(value = "写入数据源类型，快码：HDSP.DATASOURCE_TYPE", required = true)
    @NotBlank
    private String writeDatasourceType;
    @ApiModelProperty(value = "写入数据源ID,关联HDSP_CORE_DATASOURCE.DATASOURCE_ID", required = true)
    @NotNull
    private Long writeDatasourceId;
    @ApiModelProperty(value = "生成的Datax Json文件名称", required = true)
    @NotBlank
    private String jsonFileName;
    @ApiModelProperty(value = "调度任务ID,关联HDSP_DISP_JOB.JOB_ID,回写")
    private Byte[] settingInfo;
    @ApiModelProperty(value = "租户ID")
    private Long tenantId;
    @ApiModelProperty(value = "版本号", required = true)
    @NotNull
    private Long objectVersionNumber;
    @ApiModelProperty(required = true)
    @NotNull
    private Date creationDate;
    @ApiModelProperty(required = true)
    @NotNull
    private Long createdBy;
    @ApiModelProperty(required = true)
    @NotNull
    private Long lastUpdatedBy;
    @ApiModelProperty(required = true)
    @NotNull
    private Date lastUpdateDate;

}
