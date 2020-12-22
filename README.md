# DEPRECATED!!!不在维护!!!

已优化重构并迁移至datax-admin

[datax-admin传送门](https://github.com/thestyleofme/datax-admin.git)

---

# common-datax
基于阿里DataX开发一个通用导数的微服务，可以开发前台页面，根据reader和writer自动进行数据同步

本项目只限于同步数据源量很少的时候使用，若是数据源很多的情况，请参考下面的设计思路


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

例如：mysql到hive

选择mysql需要同步的表、字段等信息，输入导入到hive的库表分区等信息，不需提前在hive进行创库创表创分区，自动根据要导的mysql表以及字段类型进行创建hive库表分区，然后利用freemarker去生成json文件，使用Azkaban进行调度执行，自动创建项目、上传zip、执行流一系列操作，可在Azkaban页面进行查看。当然也提供了可直接远程python执行。

上述设计使用策略实现，只有几个数据源之间相互同步还好，如hive/mysql/oracle三个，策略模式还是不错的，但若是数据源很多的时候，策略模式不是很方便，写的类也成幂次方增加，为了优化开发易维护，只有放弃策略模式，用以下方式，代码我就不推了，有点懒。

### 设计思路（跟策略模式对比即目前的项目） 后续有时间我推一下新版的设计实现：
- 摒弃freemarker，DTO直接使用Map映射，Map里传reader、writer、setting的信息
- 定义WriterService/ReaderService接口，该接口方法处理reader/writer部分的json信息
- 一个reader/一个writer对应一个类进行处理（使用反射），专门生成reader/writer部分的json，最后加上setting部分生成成一个完整DataX的Job类

伪代码
```
DataxSyncDTO
/**
* 同步信息，包含以下三个key
* @see BaseReader 子类
* @see BaseWriter 子类
* @see Job.Setting 
*/
private Map<String, Object> sync;

ReaderService/WriterService
如可以实现MysqlReaderService/MysqlWriterService
public interface ReaderService<T extends BaseReader> {
     /**
     * 解析reader
     *
     * @param tenantId     租户id
     * @param datasourceId 数据源ID
     * @param reader       json
     * @return json
     */
    String parseReader(Long tenantId, Long datasourceId, String reader);
}

根据名称使用反射找到具体的实现类，序列化出具体的reader/writer部分json

最后组合成datax的json

最终的datax json映射类
public class Job {
    private Setting setting;
    private List<Content> content;
    public static class Setting {
        private Speed speed;
        private ErrorLimit errorLimit;
    }
    public static class Speed {
        private String record; 
        private String channel;
        private String speedByte;
    }
    public static class ErrorLimit {
        private String record;
        private String percentage;
    }
    public static class Content {
        private Reader reader;
        private Writer writer;
    }
    public static class Reader {
        private String name;
        private Object parameter;
    }
    public static class Writer {
        private String name;
        private Object parameter;
    }
}
```

### done:
- oracle、mysql、hive两两互相同步
- 本地csv文件导入到hive，支持分区
- 使用Azkaban去执行python脚本进行抽数
- 一个restful接口，可以实现所有的同步

### todo:
- 创表记录导数的历史
- json文件下载
- Azkaban定时调度等
- 数据源，mysql、hive的数据源维护，下次要导数时，不用传那么多服务器信息
- groovy脚本
---

## 说明

#### 修改配置文件application-template.yml

1. 数据源修改，根据自己项目情况进行调整

> 不要修改数据源名称，只需修改为自己的username、password、url即可
 
2. datax的信息修改
```
# 这里只要是路径，后面都加上/
datax:
  home: ${DATAX_HOME:/usr/local/DataX/target/datax/datax/}
  host: ${DATAX_HOST:datax01}
  port: 22
  # 要操作hdfs，用户要有权限
  username: ${DATAX_USERNAME:hadoop}
  password: ${DATAX_PASSWORD:hadoop}
  uploadDicPath: ${DATAX_JSON_FILE_HOME:/home/hadoop/datax/}
```
3. azkaban的url, 也可以不用azkaban，本项目默认使用azkaban进行调度
```
azkaban:
  host: ${AZKABAN_HOST:http://192.168.43.221:8081}
  username: ${AZKABAN_USERNAME:azkaban}
  password: ${AZKABAN_PASSWORD:azkaban}
```
#### 指定启动配置

> 可以重命名application-template.yml为application-dev.yml，application.yml指定生效的配置文件

```
spring:
  profiles:
    active: ${SPRING_PROFILES_ACTIVE:dev}
```
#### swagger地址
> http://localhost:10024/swagger-ui.html
---

## 使用示例

> 这里的mysql2Hive表明是mysql同步到hive，可以更换为mysql2Mysql、hive2Hive、oracle2Hive等，驼峰命名。

### 1. mysql2hive example
这里是mysql数据导入到hive，支持分区
>
> POST http://localhost:10024//v1/datax-syncs/execute
> 
> Body示例

```
{
	"syncName": "mysql2hive_test_0625_where",
	"syncDescription": "mysql2hive_test_0625_where",
	"sourceDatasourceType": "mysql",
	"sourceDatasourceId": "1",
	"writeDatasourceType": "hadoop_hive_2",
	"writeDatasourceId": "1",
	"jsonFileName": "mysql2hive_test_0625_where.json",
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
			"splitPk": "",
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
					"jdbc:mysql://hadoop04:3306/common_datax?useUnicode=true&characterEncoding=utf-8&useSSL=false"
				]
			}],
			"where": "2 > 1"
		},
		"writer": {
            "defaultFS": "hdfs://hadoop04:9000",
            "fileType": "text",
            "path": "/user/hive/warehouse/test.db/userinfo",
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
