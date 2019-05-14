package com.isacc.datax.api.controller.v1;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.app.service.AzkabanService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * <p>
 * description
 * </P>
 *
 * @author isacc 2019/05/14 13:49
 */
@RestController("azkabanController.v1")
@RequestMapping("/azkaban")
public class AzkabanController {

    private final AzkabanService azkabanService;

    public AzkabanController(AzkabanService azkabanService) {
        this.azkabanService = azkabanService;
    }

    @PostMapping("/execution")
    public ApiResult<Object> executeDataxJob(String projectName, String description, String zipPath) {
        return azkabanService.executeDataxJob(projectName, description, zipPath);
    }

}
