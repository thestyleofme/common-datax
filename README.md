# common-datax
基于阿里DataX开发一个通用导数的微服务
> 可以一起交流 qq: 283273332

---

由于阿里DataX有一些缺点：
- **不够自动化**
> 比如hdfsWriter中path必须存在，所以一般导数前要自己创表创分区，不能自动创建
- **要自己写json，去datax服务器运行**
> 程序猿都是懒的，能用代码解决的事就不要麻烦自己搬砖的时间
- **没前台页面**
> 本平台基于datax提供通用数据抽取接口，只需前台开发界面，进行一些选择，点击即可进行导数即可（不存在自动创表创分区），不需人干扰
- **不能统一管理**
> 本平台提供导数历史的查看，下载导数的json文件，查看日志，设置定时等
- ......

---

### 示例
>  mysql2hive example
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
> hive2hive example

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
		"path": "/user/hive/warehouse/test.db/userinfo_temp",
		"fileName": "userinfo_temp",
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

---

### todo:
- 主要是hive，mysql之间的导数，支持分区，还有csv导入等
- 优化，使用Redis缓存mysql/hive的所有库表
- swagger
- 创表记录导数的历史
- 配置文件属性加密，配置使用环境变量形式
- 数据源，mysql、hive的数据源维护，下次要导数时，不用传那么多服务器信息
- groovy脚本

---