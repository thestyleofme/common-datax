package com.isacc.datax.api.controller.v1;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.Hive2HiveDTO;
import com.isacc.datax.api.dto.Mysql2HiveDTO;
import com.isacc.datax.app.service.DataxHive2HiveService;
import com.isacc.datax.app.service.DataxMysql2HiveService;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * <p>
 * Datax Controller
 * </p>
 *
 * @author isacc 2019/04/29 14:13
 */
@RestController("dataxController.v1")
@RequestMapping("/datax")
public class DataxController {

    private final DataxMysql2HiveService mysql2HiveService;
    private final DataxHive2HiveService hive2HiveService;

    @Autowired
    public DataxController(DataxMysql2HiveService mysql2HiveService, DataxHive2HiveService hive2HiveService) {
        this.mysql2HiveService = mysql2HiveService;
        this.hive2HiveService = hive2HiveService;
    }

    @ApiOperation(value = "mysql条件查询数据同步到hive")
    @PostMapping("/mysql-hive-where")
    public ApiResult<Object> mysql2HiveWhere(@RequestBody Mysql2HiveDTO mysql2HiveDTO) {
        return mysql2HiveService.mysql2HiveWhere(mysql2HiveDTO);
    }

    @ApiOperation(value = "mysql自定义SQL查询数据同步到hive")
    @PostMapping("/mysql-hive-sql")
    public ApiResult<Object> mysql2HiveQuerySql(@RequestBody Mysql2HiveDTO mysql2HiveDTO) {
        return mysql2HiveService.mysql2HiveQuerySql(mysql2HiveDTO);
    }

    @ApiOperation(value = "hive之间数据同步")
    @PostMapping("/hive-hive")
    public ApiResult<Object> hive2Hive(@RequestBody Hive2HiveDTO hive2HiveDTO, String source) {
        return hive2HiveService.hive2hive(hive2HiveDTO, source);
    }

}
