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
          "name": "oraclereader",
          "parameter": {
            "username": "${oraclereaderUsername}",
            "password": "${oraclereaderPassword}",
            "splitPk": "${oraclereaderSplitPk!}",
            "column": [
              <#list oraclereaderColumn as column>
                "${column}"<#if column_has_next>,</#if>
              </#list>
            ],
            "connection": [
              <#list oraclereaderConnection as conn>
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
            "where":"${oraclereaderWhere}",
            "fetchSize": ${oraclereaderFetchSize!'1024'},
            "session": [
              <#if  oracleReaderSession??>
                <#list oracleReaderSession as session>
                  "${session}"<#if session_has_next>,</#if>
                </#list>
              </#if>
            ]
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