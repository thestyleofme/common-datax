{
  "job": {
    "setting": {
      "speed": {
        "channel": ${setting.speed.channel}
      }
    },
    "content": [
      {
        "reader": {
          "name": "hdfsreader",
          "parameter": {
            "path": "${readerPath}",
            "defaultFS": "${readerDefaultFS}",
            "column": [
            <#list readerColumns as column>
              {
              "type": "${column.type}",
              "index": "${column.index}"
              }<#if column_has_next>,</#if>
            </#list>
            ],
            "fileType": "${readerType}",
            "encoding": "UTF-8",
            "fieldDelimiter": "${fieldDelimiter}"
          }
        },
        "writer": {
          "name": "hdfswriter",
          "parameter": {
            "defaultFS": "${writerDefaultFS}",
            "fileType": "${writerType}",
            "path": "${writerPath}",
            "fileName": "${fileName}",
            "column": [
            <#list writerColumns as column>
              {
              "name": "${column.name}",
              "type": "${column.type}"
              }<#if column_has_next>,</#if>
            </#list>
            ],
            "writeMode": "${writeMode}",
            "fieldDelimiter": "${fieldDelimiter}"
          }
        }
      }
    ]
  }
}