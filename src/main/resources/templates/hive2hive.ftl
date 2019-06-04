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