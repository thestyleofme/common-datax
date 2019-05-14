package com.isacc.datax.app.service.impl;

import java.io.File;
import java.io.IOException;
import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.app.service.AzkabanService;
import com.isacc.datax.infra.config.AzkabanProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

/**
 * <p>
 * description
 * </P>
 *
 * @author isacc 2019/05/13 22:02
 */
@Service
@Slf4j
public class AzkabanServiceImpl implements AzkabanService {

    private final RestTemplate restTemplate;
    private final AzkabanProperties azkabanProperties;
    private static final String SESSION_ID = "session.id";
    private final ObjectMapper objectMapper;

    public AzkabanServiceImpl(RestTemplate restTemplate, AzkabanProperties azkabanProperties, ObjectMapper objectMapper) {
        this.restTemplate = restTemplate;
        this.azkabanProperties = azkabanProperties;
        this.objectMapper = objectMapper;
    }

    @Override
    public ApiResult<Object> executeDataxJob(String projectName, String description, String zipPath) {
        ApiResult<Object> successApiResult = ApiResult.initSuccess();
        // 登录
        ApiResult<Object> loginResult = this.login();
        if (!loginResult.getResult()) {
            return loginResult;
        }
        String sessionID = String.valueOf(loginResult.getContent());
        // 创建项目
        ApiResult<Object> createProjectResult = this.createProject(sessionID, projectName, description);
        if (!createProjectResult.getResult()) {
            return createProjectResult;
        }
        // 上传zip
        ApiResult<Object> uploadZipResult = this.uploadZip(sessionID, projectName, zipPath);
        if (!uploadZipResult.getResult()) {
            return uploadZipResult;
        }
        // 获取流
        ApiResult<Object> fetchFlowsResult = this.fetchFlows(sessionID, projectName);
        if (!fetchFlowsResult.getResult()) {
            return fetchFlowsResult;
        }
        Map fetchFlowsMap = (Map) fetchFlowsResult.getContent();
        ArrayList<String> flowList = new ArrayList<>();
        List flows = (List) fetchFlowsMap.get("flows");
        for (Object obj : flows) {
            flowList.add((String) ((Map) obj).get("flowId"));
        }
        // 执行流
        ApiResult<Object> executeFlowResult;
        List<Object> errorList = new ArrayList<>();
        for (String flow : flowList) {
            executeFlowResult = this.executeFlow(sessionID, projectName, flow);
            if (!executeFlowResult.getResult()) {
                errorList.add(executeFlowResult.getContent());
                break;
            }
        }
        if (!CollectionUtils.isEmpty(errorList)) {
            ApiResult<Object> failureApiResult = ApiResult.initFailure();
            failureApiResult.setMessage("azkaban execute flow fail!");
            failureApiResult.setContent(errorList);
        }
        successApiResult.setMessage("azkaban execute datax job success!");
        return successApiResult;
    }

    private ApiResult<Object> login() {
        ApiResult<Object> successApiResult = ApiResult.initSuccess();
        HttpHeaders hs = new HttpHeaders();
        hs.add("Content-Type", "application/x-www-form-urlencoded; charset=utf-8");
        hs.add("X-Requested-With", "XMLHttpRequest");
        LinkedMultiValueMap<String, String> linkedMultiValueMap = new LinkedMultiValueMap<>();
        linkedMultiValueMap.add("action", "login");
        linkedMultiValueMap.add("username", azkabanProperties.getUsername());
        linkedMultiValueMap.add("password", azkabanProperties.getPassword());
        HttpEntity<MultiValueMap<String, String>> httpEntity = new HttpEntity<>(linkedMultiValueMap, hs);
        try {
            Map map = restTemplate.postForObject(azkabanProperties.getHost(), httpEntity, Map.class);
            String sessionId = Optional.ofNullable(map).map(value -> String.valueOf(value.get(SESSION_ID))).orElse(null);
            successApiResult.setContent(sessionId);
        } catch (Exception e) {
            log.error("azkaban login fail,", e);
            ApiResult<Object> failureApiResult = ApiResult.initFailure();
            failureApiResult.setMessage("azkaban login fail,please check your username and password!");
            return failureApiResult;
        }
        return successApiResult;
    }

    private ApiResult<Object> createProject(String sessionID, String name, String description) {
        ApiResult<Object> successApiResult = ApiResult.initSuccess();
        HttpHeaders hs = new HttpHeaders();
        hs.add("Content-Type", "application/x-www-form-urlencoded; charset=utf-8");
        hs.add("X-Requested-With", "XMLHttpRequest");
        LinkedMultiValueMap<String, String> linkedMultiValueMap = new LinkedMultiValueMap<>();
        linkedMultiValueMap.add(SESSION_ID, sessionID);
        linkedMultiValueMap.add("action", "create");
        linkedMultiValueMap.add("name", name);
        linkedMultiValueMap.add("description", description);
        HttpEntity<MultiValueMap<String, String>> httpEntity = new HttpEntity<>(linkedMultiValueMap, hs);
        try {
            String result = restTemplate.postForObject(azkabanProperties.getHost() + "/manager", httpEntity, String.class);
            Map map = objectMapper.readValue(result, Map.class);
            successApiResult.setContent(map);
        } catch (IOException e) {
            log.error("azkaban create project fail,", e);
            ApiResult<Object> failureApiResult = ApiResult.initFailure();
            failureApiResult.setMessage("create azkaban project fail," + e.getMessage());
            return failureApiResult;
        }
        return successApiResult;
    }

    private ApiResult<Object> uploadZip(String sessionID, String name, String zipPath) {
        ApiResult<Object> successApiResult = ApiResult.initSuccess();
        FileSystemResource resource = new FileSystemResource(new File(zipPath));
        LinkedMultiValueMap<String, Object> linkedMultiValueMap = new LinkedMultiValueMap<>();
        linkedMultiValueMap.add(SESSION_ID, sessionID);
        linkedMultiValueMap.add("ajax", "upload");
        linkedMultiValueMap.add("project", name);
        linkedMultiValueMap.add("file", resource);
        try {
            String result = restTemplate.postForObject(azkabanProperties.getHost() + "/manager", linkedMultiValueMap, String.class);
            Map map = objectMapper.readValue(result, Map.class);
            successApiResult.setContent(map);
        } catch (IOException e) {
            log.error("azkaban upload zip file fail,", e);
            ApiResult<Object> failureApiResult = ApiResult.initFailure();
            failureApiResult.setMessage("azkaban upload zip file fail," + e.getMessage());
            return failureApiResult;
        }
        return successApiResult;
    }

    private ApiResult<Object> fetchFlows(String sessionID, String name) {
        ApiResult<Object> successApiResult = ApiResult.initSuccess();
        try {
            String result = restTemplate.getForObject(String.format("%s/manager?session.id=%s&ajax=fetchprojectflows&project=%s",
                    azkabanProperties.getHost(),
                    sessionID,
                    name), String.class);
            Map map = objectMapper.readValue(result, Map.class);
            successApiResult.setContent(map);
        } catch (IOException e) {
            log.error("azkaban fetch flows fail,", e);
            ApiResult<Object> failureApiResult = ApiResult.initFailure();
            failureApiResult.setMessage("azkaban fetch flows fail," + e.getMessage());
            return failureApiResult;
        }
        return successApiResult;
    }

    private ApiResult<Object> executeFlow(String sessionID, String name, String flow) {
        ApiResult<Object> successApiResult = ApiResult.initSuccess();
        try {
            String result = restTemplate.getForObject(String.format("%s/executor?session.id=%s&ajax=executeFlow&project=%s&flow=%s",
                    azkabanProperties.getHost(),
                    sessionID,
                    name,
                    flow), String.class);
            Map map = objectMapper.readValue(result, Map.class);
            successApiResult.setContent(map);
        } catch (IOException e) {
            log.error("azkaban execute flow fail,", e);
            ApiResult<Object> failureApiResult = ApiResult.initFailure();
            failureApiResult.setMessage("azkaban execute flow fail," + e.getMessage());
            return failureApiResult;
        }
        return successApiResult;
    }


}
