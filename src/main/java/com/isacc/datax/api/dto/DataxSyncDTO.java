package com.isacc.datax.api.dto;

import java.util.Date;

import io.swagger.annotations.ApiModel;
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
public class DataxSyncDTO {

    public static final String FIELD_SYNC_ID = "syncId";
    public static final String FIELD_SYNC_NAME = "syncName";
    public static final String FIELD_SYNC_DESCRIPTION = "syncDescription";
    public static final String FIELD_SOURCE_DATASOURCE_TYPE = "sourceDatasourceType";
    public static final String FIELD_SOURCE_DATASOURCE_ID = "sourceDatasourceId";
    public static final String FIELD_WRITE_DATASOURCE_TYPE = "writeDatasourceType";
    public static final String FIELD_WRITE_DATASOURCE_ID = "writeDatasourceId";
    public static final String FIELD_JSON_FILE_NAME = "jsonFileName";
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

    private Long syncId;
    private String syncName;
    private String syncDescription;
    private String sourceDatasourceType;
    private Long sourceDatasourceId;
    private String writeDatasourceType;
    private Long writeDatasourceId;
    private String jsonFileName;
    private Byte[] settingInfo;
    /**
     * 租户ID
     */
    private Long tenantId;
    private Long objectVersionNumber;
    private Date creationDate;
    private Long createdBy;
    private Long lastUpdatedBy;
    private Date lastUpdateDate;

    /**
     * 支持的datax同步任务
     */
    private Mysql2Mysql mysql2Mysql;
    private Mysql2Hive mysql2Hive;
    private Mysql2Oracle mysql2Oracle;
    private Hive2Hive hive2Hive;
    private Hive2Mysql hive2Mysql;
    private Hive2Oracle hive2Oracle;
    private Oracle2Hive oracle2Hive;
    private Oracle2Oracle oracle2Oracle;
    private Oracle2Mysql oracle2Mysql;

}
