package com.isacc.datax.api.controller.v1;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.hive.HiveInfoDTO;
import com.isacc.datax.app.service.HiveService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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
	public ApiResult<String> createTable(@RequestBody HiveInfoDTO hiveInfoDTO) {
		return hiveService.createTable(hiveInfoDTO);
	}
}
