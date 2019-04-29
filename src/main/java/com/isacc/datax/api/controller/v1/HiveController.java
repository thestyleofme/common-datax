package com.isacc.datax.api.controller.v1;

import javax.validation.constraints.NotNull;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.HiveInfoDTO;
import com.isacc.datax.app.service.HiveService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * <p>
 * Hive CURD
 * </p>
 *
 * @author isacc 2019/04/28 19:34
 */
@RestController("hiveController.v1")
@RequestMapping("/hive")
public class HiveController {

	private final HiveService hiveService;

	@Autowired
	public HiveController(HiveService hiveService) {
		this.hiveService = hiveService;
	}


	@PostMapping("/table")
	public ApiResult<Object> createTable(@RequestBody HiveInfoDTO hiveInfoDTO) {
		return hiveService.createTable(hiveInfoDTO);
	}

	@DeleteMapping("/table")
	public ApiResult<Object> deleteTable(@RequestBody HiveInfoDTO hiveInfoDTO) {
		return hiveService.deleteTable(hiveInfoDTO);
	}

	@GetMapping("/database")
	public ApiResult<Object> createTable(@NotNull String databaseName) {
		return hiveService.createDatabase(databaseName);
	}

	@DeleteMapping("/database")
	public ApiResult<Object> deleteDatabase(@NotNull String databaseName) {
		return hiveService.deleteDatabase(databaseName);
	}


}
