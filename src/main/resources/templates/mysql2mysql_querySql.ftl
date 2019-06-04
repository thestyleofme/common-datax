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
            "connection": [
            <#list mysqlreaderConnection as conn>
              {
              "querySql": [
              <#list conn.querySql as sql>
                "${sql}"<#if sql_has_next>,</#if>
              </#list>
              ],
              "jdbcUrl": [
              <#list conn.jdbcUrl as url>
                "${url}"<#if url_has_next>,</#if>
              </#list>
              ]
              }<#if conn_has_next>,</#if>
            </#list>
            ]
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
              <#if  mysqlwriterPostSql??>
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