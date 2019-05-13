# common-datax
基于阿里DataX开发一个通用导数的微服务

---

由于阿里DataX有一些缺点：
- **不够自动化**
- **需要手写json**
- **需要手动运行job**

搬砖的时间很宝贵，所以：
- **提供通用数据抽取restful接口**
- **HDFS自动创库创表创分区**
- **自动创建json文件**
- **自动python执行job**

### done:
- mysql到hive的通用数据抽取，支持导入到Hive分区
- hive到hive的通用数据抽取，支持Hive分区
- 本地csv文件导入到hive，支持分区

### todo:
- 主要是hive，mysql之间的导数，支持分区，还有csv导入等
- 创表记录导数的历史
- 配置文件属性加密，配置使用环境变量形式
- json文件下载
- Azkaban定时调度等
- 优化，使用Redis缓存mysql/hive的所有库表
- swagger
- 数据源，mysql、hive的数据源维护，下次要导数时，不用传那么多服务器信息
- groovy脚本
---

## 示例
### 1. mysql2hive example
这里是mysql数据导入到hive，支持分区
>
> POST http://localhost:10024/datax/mysql-hive-where 
> 
> Body示例

```
{
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
		"name": "mysqlreader",
		"username": "root",
		"password": "root",
		"column": [
			"id",
			"username"
		],
		"connection": [{
			"table": [
				"userinfo"
			],
			"jdbcUrl": [
				"jdbc:mysql://hadoop04:3306/sudu?useUnicode=true&characterEncoding=utf-8&useSSL=false"
			]
		}],
		"where": "2 > 1"
	},
	"writer": {
		"name": "hdfswriter",
		"defaultFS": "hdfs://hadoop04:9000",
		"fileType": "text",
		"path": "/user/hive/warehouse/test.db/userinfo",
		"fileName": "userinfo",
		"column": [{
				"name": "id",
				"type": "BIGINT"
			},
			{
				"name": "username",
				"type": "STRING"
			}
		],
		"writeMode": "append",
		"fieldDelimiter": "\\t"
	}
}
```
> path可以更换为分区的hdfs路径，不需提前创建分区，自动创建，例如：

```
"path": "/user/hive/warehouse/test.db/userinfo_dts/dt1=A1/dt2=B2"
```
> 这里会在hive里自动创建userinfo_dts分区表，有两个分区字段，然后会将数据导入到这里的dt1=A1,dt2=B2分区下
---

### 2. hive2hive example
##### 1. 这里是将hive里userinfo这张表导入到一张hive分区表中
> POST http://localhost:10024/datax/hive-hive
> 
> Body示例
```
{
	"setting": {
		"speed": {
			"channel": 3
		}
	},
	"reader": {
		"name": "hdfsreader",
		"path": "/user/hive/warehouse/test.db/userinfo",
		"defaultFS": "hdfs://hadoop04:9000",
		"column": [{
				"type": "long",
				"index": 0
			},
			{
				"type": "string",
				"index": 1
			}
		],
		"fileType": "text",
		"encoding": "UTF-8",
		"fieldDelimiter": "\\t"
	},
	"writer": {
		"name": "hdfswriter",
		"defaultFS": "hdfs://hadoop04:9000",
		"fileType": "text",
		"path": "/user/hive/warehouse/test.db/userinfo_temp_dts/dt1=A1/dt2=B1",
		"fileName": "userinfo_temp_dts",
		"column": [{
				"name": "id",
				"type": "BIGINT"
			},
			{
				"name": "username",
				"type": "STRING"
			}
		],
		"writeMode": "append",
		"fieldDelimiter": "\\t"
	}
}
```
##### 2. csv导入到hive表示例
> POST http://localhost:10024/datax/hive-hive?source=C:/test/userinfo_csv.csv
>
> Body示例

```
{
	"setting": {
		"speed": {
			"channel": 3
		}
	},
	"reader": {
		"name": "hdfsreader",
		"path": "/user/hive/warehouse/test/userinfo_csv/userinfo_csv.csv",
		"defaultFS": "hdfs://hadoop04:9000",
		"column": [{
				"type": "long",
				"index": 0
			},
			{
				"type": "string",
				"index": 1
			}
		],
		"fileType": "csv",
		"encoding": "UTF-8",
		"fieldDelimiter": ","
	},
	"writer": {
		"name": "hdfswriter",
		"defaultFS": "hdfs://hadoop04:9000",
		"fileType": "text",
		"path": "/user/hive/warehouse/test.db/userinfo_csv",
		"fileName": "userinfo_csv",
		"column": [{
				"name": "id",
				"type": "BIGINT"
			},
			{
				"name": "username",
				"type": "STRING"
			}
		],
		"writeMode": "append",
		"fieldDelimiter": ","
	}
}
```

