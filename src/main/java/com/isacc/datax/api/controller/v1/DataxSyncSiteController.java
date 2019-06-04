package com.isacc.datax.api.controller.v1;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.DataxSyncDTO;
import com.isacc.datax.app.service.DataxSyncService;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.*;

/**
 * 数据同步表 管理 API
 *
 * @author isacc 2019-05-17 14:07:48
 */
@RestController("dataxSyncSiteController.v1")
@RequestMapping("/v1/datax-syncs")
public class DataxSyncSiteController {

    private final DataxSyncService dataxSyncService;

    public DataxSyncSiteController(DataxSyncService dataxSyncService) {
        this.dataxSyncService = dataxSyncService;
    }

    @ApiOperation(value = "获取DataX任务执行命令")
    @GetMapping("/datax-command")
    public String generateDataxCommand() {
        return dataxSyncService.generateDataxCommand();
    }

    @ApiOperation(value = "执行datax同步任务")
    @PostMapping("/execute")
    public ApiResult<Object> execute(@RequestBody DataxSyncDTO dataxSyncDTO) {
        return dataxSyncService.execute(dataxSyncDTO);
    }

}
