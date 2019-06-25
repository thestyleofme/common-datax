package com.isacc.datax.app.service.impl;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.app.service.AzkabanService;
import com.isacc.datax.infra.config.AzkabanProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.FileSystemResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
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
 * @author isacc 2019/05/20 14:02
 */
@Service
@Slf4j
public class AzkabanServiceImpl extends BaseDataxServiceImpl implements AzkabanService {

    private final RestTemplate cusRestTemplate;
    private final AzkabanProperties azkabanProperties;
    private static final String SESSION_ID = "session.id";
    private static final String ERROR = "error";
    private final ObjectMapper objectMapper;
    private final StringRedisTemplate stringRedisTemplate;

    public AzkabanServiceImpl(RestTemplate cusRestTemplate, AzkabanProperties azkabanProperties, ObjectMapper objectMapper, StringRedisTemplate stringRedisTemplate) {
        this.cusRestTemplate = cusRestTemplate;
        this.azkabanProperties = azkabanProperties;
        this.objectMapper = objectMapper;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public ApiResult<Object> executeDataxJob(String projectName, String description, String zipPath) {
        ApiResult<Object> successApiResult = ApiResult.initSuccess();
        // 登录
        ApiResult<Object> loginResult = this.login();
        if (!loginResult.getResult()) {
            return loginResult;
        }
        String sessionId = String.valueOf(loginResult.getContent());
        // 创建项目
        ApiResult<Object> createProjectResult = this.createProject(sessionId, projectName, description);
        if (!createProjectResult.getResult()) {
            return createProjectResult;
        }
        // 上传zip
        ApiResult<Object> uploadZipResult = this.uploadZip(sessionId, projectName, zipPath);
        if (!uploadZipResult.getResult()) {
            return uploadZipResult;
        }
        // 获取流
        ApiResult<Object> fetchFlowsResult = this.fetchFlows(sessionId, projectName);
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
        List<Object> execList = new ArrayList<>();
        for (String flow : flowList) {
            executeFlowResult = this.executeFlow(sessionId, projectName, flow);
            if (!executeFlowResult.getResult()) {
                errorList.add(executeFlowResult.getContent());
                break;
            } else {
                execList.add(executeFlowResult.getContent());
            }
        }
        if (!CollectionUtils.isEmpty(errorList)) {
            ApiResult<Object> failureApiResult = ApiResult.initFailure();
            failureApiResult.setMessage(String.format("azkaban project: %s execute datax job fail!", projectName));
            failureApiResult.setContent(errorList);
        }
        successApiResult.setMessage(String.format("azkaban project: %s execute datax job success!", projectName));
        successApiResult.setContent(execList);
        return successApiResult;
    }

    private ApiResult<Object> login() {
        ApiResult<Object> successApiResult = ApiResult.initSuccess();
        ApiResult<Object> failureApiResult = ApiResult.initFailure();
        LinkedMultiValueMap<String, String> linkedMultiValueMap = new LinkedMultiValueMap<>();
        linkedMultiValueMap.add("action", "login");
        linkedMultiValueMap.add("username", azkabanProperties.getUsername());
        linkedMultiValueMap.add("password", azkabanProperties.getPassword());
        HttpEntity<MultiValueMap<String, String>> httpEntity = new HttpEntity<>(linkedMultiValueMap, this.azkabanHeaders());
        try {
            String sessionIdCache = stringRedisTemplate.opsForValue().get(SESSION_ID);
            if (Objects.isNull(sessionIdCache)) {
                Map map = cusRestTemplate.postForObject(azkabanProperties.getHost(), httpEntity, Map.class);
                String sessionId = Optional.ofNullable(map).map(value -> String.valueOf(value.get(SESSION_ID))).orElse(null);
                if (!Objects.isNull(sessionId)) {
                    stringRedisTemplate.opsForValue().set(SESSION_ID, sessionId, 1L, TimeUnit.DAYS);
                }
                if (!Objects.isNull(map) && map.containsKey(ERROR)) {
                    failureApiResult.setContent(map.get(ERROR));
                    failureApiResult.setMessage("azkaban login fail,please check your username and password!");
                    return failureApiResult;
                }
                successApiResult.setContent(sessionId);
            } else {
                successApiResult.setContent(sessionIdCache);
            }
        } catch (Exception e) {
            log.error("azkaban login fail,", e);
            failureApiResult.setMessage("azkaban login fail,please check your username and password!" + e.getMessage());
            return failureApiResult;
        }
        return successApiResult;
    }

    private ApiResult<Object> createProject(String sessionId, String name, String description) {
        LinkedMultiValueMap<String, String> linkedMultiValueMap = new LinkedMultiValueMap<>();
        linkedMultiValueMap.add(SESSION_ID, sessionId);
        linkedMultiValueMap.add("action", "create");
        linkedMultiValueMap.add("name", name);
        linkedMultiValueMap.add("description", description);
        HttpEntity<MultiValueMap<String, String>> httpEntity = new HttpEntity<>(linkedMultiValueMap, this.azkabanHeaders());
        String result = cusRestTemplate.postForObject(String.format("%s/manager", azkabanProperties.getHost()), httpEntity, String.class);
        return checkRequestResult(result);
    }

    private ApiResult<Object> uploadZip(String sessionId, String name, String zipPath) {
        FileSystemResource fileAsResource = new FileSystemResource(zipPath);
        LinkedMultiValueMap<String, Object> linkedMultiValueMap = new LinkedMultiValueMap<>();
        linkedMultiValueMap.add(SESSION_ID, sessionId);
        linkedMultiValueMap.add("ajax", "upload");
        linkedMultiValueMap.add("project", name);
        linkedMultiValueMap.add("file", fileAsResource);
        String result = cusRestTemplate.postForObject(String.format("%s/manager", azkabanProperties.getHost()), linkedMultiValueMap, String.class);
        return checkRequestResult(result);
    }

    private ApiResult<Object> fetchFlows(String sessionId, String name) {
        String result = cusRestTemplate.getForObject(String.format("%s/manager?session.id=%s&ajax=fetchprojectflows&project=%s",
                azkabanProperties.getHost(),
                sessionId,
                name), String.class);
        return this.checkRequestResult(result);
    }

    private ApiResult<Object> executeFlow(String sessionId, String name, String flow) {
        String result = cusRestTemplate.getForObject(String.format("%s/executor?session.id=%s&ajax=executeFlow&project=%s&flow=%s",
                azkabanProperties.getHost(),
                sessionId,
                name,
                flow), String.class);
        return checkRequestResult(result);
    }

    private HttpHeaders azkabanHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", MediaType.APPLICATION_FORM_URLENCODED_VALUE);
        headers.add("X-Requested-With", "XMLHttpRequest");
        return headers;
    }

    private ApiResult<Object> checkRequestResult(String result) {
        ApiResult<Object> successApiResult = ApiResult.initSuccess();
        ApiResult<Object> failureApiResult = ApiResult.initFailure();
        try {
            Map map = objectMapper.readValue(result, Map.class);
            if (map != null && map.containsKey(ERROR)) {
                failureApiResult.setMessage(String.valueOf(map.get(ERROR)));
                return failureApiResult;
            }
            successApiResult.setContent(map);
        } catch (IOException e) {
            log.error("something exceptional has happened,", e);
            failureApiResult.setMessage(e.getMessage());
            return failureApiResult;
        }
        return successApiResult;
    }
}
