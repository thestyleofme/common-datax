package com.isacc.datax.api.controller.v1;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.DataxSyncDTO;
import com.isacc.datax.app.service.DataxSyncService;
import com.isacc.datax.domain.entity.DataxSync;
import com.isacc.datax.domain.repository.DataxSyncRepository;
import com.isacc.datax.infra.constant.Constants;
import io.swagger.annotations.ApiOperation;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.SortDefault;
import org.springframework.web.bind.annotation.*;
import springfox.documentation.annotations.ApiIgnore;

/**
 * 数据同步表 管理 API
 *
 * @author isacc 2019-05-17 14:07:48
 */
@RestController("dataxSyncSiteController.v1")
@RequestMapping("/v1/datax-syncs")
public class DataxSyncSiteController {

    private final DataxSyncService dataxSyncService;
    private final DataxSyncRepository dataxSyncRepository;

    public DataxSyncSiteController(DataxSyncService dataxSyncService, DataxSyncRepository dataxSyncRepository) {
        this.dataxSyncService = dataxSyncService;
        this.dataxSyncRepository = dataxSyncRepository;
    }

    @ApiOperation(value = "数据同步表列表")
    @GetMapping
    public IPage<DataxSyncDTO> list(DataxSyncDTO dataxSyncDTO, @ApiIgnore @SortDefault(value = DataxSync.FIELD_SYNC_ID,
            direction = Sort.Direction.DESC) Page<DataxSync> dataxSyncPage) {
        dataxSyncDTO.setTenantId(Constants.DEFAULT_TENANT_ID);
        return dataxSyncRepository.pageAndSortDTO(dataxSyncPage, dataxSyncDTO);
    }

    @ApiOperation(value = "数据同步表明细")
    @GetMapping("/{syncId}")
    public DataxSyncDTO detail(@PathVariable Long syncId) {
        return dataxSyncRepository.selectByPrimaryKey(syncId);
    }

    @ApiOperation(value = "新增数据同步任务")
    @PostMapping
    public DataxSyncDTO create(@RequestBody DataxSyncDTO dataxSyncDTO) {
        dataxSyncDTO.setTenantId(Constants.DEFAULT_TENANT_ID);
        return dataxSyncRepository.insertSelectiveDTO(dataxSyncDTO);
    }

    @ApiOperation(value = "获取DataX任务执行命令")
    @GetMapping("/datax-command")
    public String generateDataxCommand() {
        return dataxSyncService.generateDataxCommand();
    }

    @ApiOperation(value = "执行datax同步任务")
    @PostMapping("/execute")
    public ApiResult<Object> execute(@RequestBody DataxSyncDTO dataxSyncDTO) {
        dataxSyncDTO.setTenantId(Constants.DEFAULT_TENANT_ID);
        return dataxSyncService.execute(dataxSyncDTO);
    }

    @ApiOperation(value = "校验datax同步任务名称以及json文件名称是否重复")
    @PostMapping("/check")
    public ApiResult<Object> checkSyncNameAndJsonFileName(@RequestBody DataxSyncDTO dataxSyncDTO) {
        dataxSyncDTO.setTenantId(Constants.DEFAULT_TENANT_ID);
        return dataxSyncService.checkSyncNameAndJsonFileName(dataxSyncDTO);
    }

    @ApiOperation(value = "修改数据同步任务")
    @PutMapping
    public DataxSyncDTO update(@RequestBody DataxSyncDTO dataxSyncDTO) {
        dataxSyncDTO.setTenantId(Constants.DEFAULT_TENANT_ID);
        return dataxSyncRepository.updateSelectiveDTO(dataxSyncDTO);
    }

    @ApiOperation(value = "删除数据同步表")
    @DeleteMapping
    public ApiResult<Object> remove(@RequestBody DataxSyncDTO dataxSyncDTO) {
        dataxSyncDTO.setTenantId(Constants.DEFAULT_TENANT_ID);
        return dataxSyncService.deleteDataxSync(dataxSyncDTO);
    }

}
