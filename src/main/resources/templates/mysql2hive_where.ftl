{
  "job": {
    "setting": {
      "speed": {
        "channel": ${(setting.speed.channel)!"3"}
      },
      "errorLimit": {
        "record": ${(setting.errorLimit.record)!"0"},
        "percentage": ${(setting.errorLimit.percentage)!"0.02"}
      }
    },
    "content": [
      {
        "reader": {
          "name": "mysqlreader",
          "parameter": {
            "username": "${mysqlreaderUsername}",
            "password": "${mysqlreaderPassword}",
            "splitPk": "${mysqlreaderSplitPk!}",
            "column": [
              <#list mysqlreaderColumn as column>
                "${column}"<#if column_has_next>,</#if>
              </#list>
            ],
            "connection": [
              <#list mysqlreaderConnection as conn>
                {
                  "table": [
                    <#list conn.table as tbl>
                      "${tbl}"<#if tbl_has_next>,</#if>
                    </#list>
                  ],
                  "jdbcUrl": [
                    <#list conn.jdbcUrl as url>
                      "${url}"<#if url_has_next>,</#if>
                    </#list>
                  ]
                }<#if conn_has_next>,</#if>
              </#list>
            ],
            "where":"${mysqlreaderWhere}"
          }
        },
        "writer": {
          "name": "hdfswriter",
          "parameter": {
            "defaultFS": "${hdfswriterDefaultFS}",
            "fileType": "${hdfswriterFileType}",
            "path": "${hdfswriterPath}",
            "fileName": "${hdfswriterFileName}",
            "column": [
              <#list hdfswriterColumn as column>
                  {
                  "name": "${column.name}",
                  "type": "${column.type}"
                  }<#if column_has_next>,</#if>
              </#list>
            ],
            "writeMode": "${writeMode}",
            "fieldDelimiter": "${hdfswriterFieldDelimiter}",
            "compress": "${hdfswriterCompress!}",
            "hadoopConfig": {
              <#if  hdfswriterHadoopConfig??>
                <#list hdfswriterHadoopConfig as key, value>
                  "${key}": "${value}"<#if key_has_next>,</#if>
                </#list>
              </#if>
            },
            "haveKerberos": ${hdfswriterHaveKerberos?then("true","false")},
            "kerberosKeytabFilePath": "${hdfswriterKerberosKeytabFilePath!}",
            "kerberosPrincipal": "${hdfswriterKerberosPrincipal!}"
          }
        }
      }
    ]
  }
}