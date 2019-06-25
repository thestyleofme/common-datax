# common-datax
基于阿里DataX开发一个通用导数的微服务，配合web ui使用

---

由于阿里DataX有一些缺点：
- **不够自动化**
- **需要手写json**
- **需要手动运行job**

搬砖的时间很宝贵，所以：
- **提供通用数据抽取restful接口**
- **HDFS自动创库创表创分区**
- **利用freemarker模板自动创建json文件**
- **自动python执行job**
- **集成Azkaban进行调度管理**

可以开发前台页面，根据reader和writer去自动进行数据同步

例如：mysql到hive

选择mysql需要同步的表、字段等信息，输入导入到hive的库表分区等信息，不需提前在hive进行创库创表创分区，自动根据要导的mysql表以及字段类型进行创建hive库表分区，然后利用freemarker去生成json文件，使用Azkaban进行调度执行，自动创建项目、上传zip、执行流一系列操作，可在Azkaban页面进行查看。当然也提供了可直接远程python执行。

### done:
- oracle、mysql、hive两两互相同步
- 本地csv文件导入到hive，支持分区
- 使用Azkaban去执行python脚本进行抽数
- 一个restful接口，可以实现所有的同步

### todo:
- 主要是hive，mysql之间的导数，支持分区，还有csv导入等
- 创表记录导数的历史
- 配置文件属性加密，配置使用环境变量形式
- json文件下载
- Azkaban定时调度等
- swagger
- 数据源，mysql、hive的数据源维护，下次要导数时，不用传那么多服务器信息
- groovy脚本
---

## 示例

> 这里的mysql2Hive表明是mysql同步到hive，可以更换为mysql2Mysql、hive2Hive、oracle2Hive等，驼峰命名。

### 1. mysql2hive example
这里是mysql数据导入到hive，支持分区
>
> POST http://localhost:10024//v1/datax-syncs/execute
> 
> Body示例

```
{
	"syncName": "mysql2hive_test_0604_where",
	"syncDescription": "mysql2hive_test_0604_where",
	"sourceDatasourceType": "mysql",
	"sourceDatasourceId": "1",
	"writeDatasourceType": "hadoop_hive_2",
	"writeDatasourceId": "1",
	"jsonFileName": "mysql2hive_test_0604_where.json",
	"mysql2Hive": {
		"setting": {
			"speed": {
				"channel": 3
			},
			"errorLimit": {
				"record": 0,
				"percentage": 0.02
			}
		},
		"reader": {
			"username": "root",
			"password": "root",
			"splitPk": "",
			"column": [
				"id",
				"username"
			],
			"connection": [{
				"table": [
					"userinfo"
				],
				"jdbcUrl": [
					"jdbc:mysql://192.168.11.227:3306/hdsp_datax?useUnicode=true&characterEncoding=utf-8&useSSL=false"
				]
			}],
			"where": "2 > 1"
		},
		"writer": {
            "defaultFS": "hdfs://hdmp01.novalocal:8020",
            "fileType": "text",
            "path": "/warehouse/tablespace/managed/hive/hdsp_test.db/userinfo",
            "fileName": "userinfo",
            "column": [
                {
                "name": "id",
                "type": "BIGINT"
                },
                {
                "name": "username",
                "type": "STRING"
                }
            ],
            "writeMode": "append",
            "fieldDelimiter": "\t",
            "compress": "",
            "hadoopConfig": {
            },
            "haveKerberos": false,
            "kerberosKeytabFilePath": "",
            "kerberosPrincipal": ""
		}
	}
}
```
> path可以更换为分区的hdfs路径，不需提前创建分区，自动创建，例如：

```
"path": "/user/hive/warehouse/test.db/userinfo_dts/dt1=A1/dt2=B2"
```
> 这里会在hive里自动创建userinfo_dts分区表，有两个分区字段，然后会将数据导入到这里的dt1=A1,dt2=B2分区下
