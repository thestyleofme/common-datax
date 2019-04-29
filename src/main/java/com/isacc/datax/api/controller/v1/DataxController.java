package com.isacc.datax.api.controller.v1;

import com.isacc.datax.api.dto.Mysql2HiveDTO;
import org.springframework.web.bind.annotation.*;

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

	@PostMapping
	public void mysqlImport(@RequestBody Mysql2HiveDTO mysql2HiveDTO) {
		System.out.println();
	}

}
