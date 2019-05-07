# common-datax
基于阿里DataX做一个通用导数平台

### todo:
- 主要是hive，mysql之间的导数，支持分区，还有csv导入等
- 优化，使用Redis缓存mysql/hive的所有库表
- swagger
- 创表记录导数的历史
- 配置文件属性加密，配置使用环境变量形式
- 数据源，mysql、hive的数据源维护，下次要导数时，不用传那么多服务器信息
- groovy脚本