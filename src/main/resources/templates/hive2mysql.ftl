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
          "name": "hdfsreader",
          "parameter": {
            "path": "${hdfsreaderPath}",
            "defaultFS": "${hdfsreaderDefaultFS}",
            "column": [
              <#list hdfsreaderColumn as column>
                {
                "type": "${column.type}",
                "index": "${column.index}"
                }<#if column_has_next>,</#if>
              </#list>
            ],
            "fileType": "${hdfsreaderFileType}",
            "fieldDelimiter": "${hdfsreaderFieldDelimiter}",
            "nullFormat": "${hdfsreaderNullFormat!}",
            "compress": "${hdfsreaderCompress!}",
            "hadoopConfig": {
              <#if  hdfsreaderHadoopConfig??>
                <#list hdfsreaderHadoopConfig as key, value>
                  "${key}": "${value}"<#if key_has_next>,</#if>
                </#list>
              </#if>
            },
            "csvReaderConfig": {
              <#if  hdfsreaderCsvReaderConfig??>
                <#list hdfsreaderCsvReaderConfig as key, value>
                  "${key}": "${value}"<#if key_has_next>,</#if>
                </#list>
              </#if>
            },
            "haveKerberos": ${hdfsreaderHaveKerberos?then("true","false")},
            "kerberosKeytabFilePath": "${hdfsreaderKerberosKeytabFilePath!}",
            "kerberosPrincipal": "${hdfsreaderKerberosPrincipal!}"
          }
        },
        "writer": {
          "name": "mysqlwriter",
          "parameter": {
            "writeMode": "${writeMode}",
            "username": "${mysqlwriterUsername}",
            "password": "${mysqlwriterPassword}",
            "batchSize": ${mysqlwriterBatchSize!'1024'},
            "column": [
              <#list mysqlwriterColumn as column>
                "${column}"<#if column_has_next>,</#if>
              </#list>
            ],
            "session": [
              <#if mysqlwriterSession??>
                <#list mysqlwriterSession as session>
                  "${session}"<#if session_has_next>,</#if>
                </#list>
              </#if>
            ],
            "preSql": [
              <#if mysqlwriterPreSql??>
                <#list mysqlwriterPreSql as preSql>
                  "${preSql}"<#if preSql_has_next>,</#if>
                </#list>
              </#if>
            ],
            "connection": [
              <#list mysqlwriterConnection as conn>
                {
                "table": [
                  <#list conn.table as tbl>
                    "${tbl}"<#if tbl_has_next>,</#if>
                  </#list>
                ],
                "jdbcUrl": "${conn.jdbcUrl}"
                }<#if conn_has_next>,</#if>
              </#list>
            ],
            "postSql": [
              <#if mysqlwriterPostSql??>
                <#list mysqlwriterPostSql as postSql>
                  "${postSql}"<#if postSql_has_next>,</#if>
                </#list>
              </#if>
            ]
          }
        }
      }
    ]
  }
}