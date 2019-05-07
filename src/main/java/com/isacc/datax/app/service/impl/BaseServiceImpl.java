package com.isacc.datax.app.service.impl;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.app.service.BaseService;
import com.isacc.datax.infra.config.DataxProperties;
import com.isacc.datax.infra.util.SftpUtil;
import com.jcraft.jsch.JSchException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileItemFactory;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.commons.CommonsMultipartFile;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * description
 *
 * @author HP_USER 2019/05/07 23:26
 */
@Service
@Slf4j
public class BaseServiceImpl implements BaseService {

    @Override
    public ApiResult<Object> file2MultipartFile(File file) {
        final ApiResult<Object> successApiResult = ApiResult.initSuccess();
        FileItemFactory factory = new DiskFileItemFactory(16, null);
        FileItem fileItem = factory.createItem(file.getName(), "text/html", true, file.getName());
        int bytesRead;
        int bytes = 8 * 1024;
        byte[] buffer = new byte[bytes];
        try (FileInputStream fis = new FileInputStream(file);
             OutputStream os = fileItem.getOutputStream()) {
            while ((bytesRead = fis.read(buffer, 0, bytes)) != -1) {
                os.write(buffer, 0, bytesRead);
            }
            MultipartFile multipartFile = new CommonsMultipartFile(fileItem);
            successApiResult.setContent(multipartFile);
            return successApiResult;
        } catch (IOException e) {
            final ApiResult<Object> failureApiResult = ApiResult.initFailure();
            log.error("file to MultipartFile error,", e);
            failureApiResult.setMessage(e.getMessage());
            return failureApiResult;
        }
    }

    /**
     * 上传文件到datax服务器
     *
     * @param file            上传的文件
     * @param dataxProperties dataxProperties
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     */
    @Override
    public ApiResult<Object> uploadFile(MultipartFile file, DataxProperties dataxProperties) {
        final ApiResult<Object> failureApiResult = ApiResult.initFailure();
        final ApiResult<Object> successApiResult = ApiResult.initSuccess();
        if (file.isEmpty()) {
            failureApiResult.setMessage("the select file is empty!");
            return failureApiResult;
        }
        String fileName = file.getOriginalFilename();
        try (SftpUtil util = new SftpUtil()) {
            util.connectServerUseSftp(getDataxInfo(dataxProperties));
            util.uploadFile(dataxProperties.getUploadDicPath() + '/' + fileName, file.getInputStream());
        } catch (Exception e) {
            log.error("upload json file error,", e);
            failureApiResult.setMessage("upload file error!");
            failureApiResult.setContent(e.getMessage());
            return failureApiResult;
        }
        successApiResult.setContent(fileName);
        return successApiResult;
    }

    @Override
    public String[] getDataxInfo(DataxProperties dataxProperties) {
        String ip = dataxProperties.getHost();
        String port = dataxProperties.getPort();
        String username = dataxProperties.getUsername();
        String password = dataxProperties.getPassword();
        return new String[]{ip, port, username, password};
    }

    @Override
    public ApiResult<Object> execCommand(DataxProperties dataxProperties, String jsonFileName) {
        final ApiResult<Object> successApiResult = ApiResult.initSuccess();
        String command = "source /etc/profile;" + dataxProperties.getHome() + "bin/datax.py " + dataxProperties.getUploadDicPath() + jsonFileName;
        try (SftpUtil util = new SftpUtil()) {
            util.connectServerUseExec(command, getDataxInfo(dataxProperties));
        } catch (JSchException | IOException e) {
            log.error("command execution failed,", e);
            final ApiResult<Object> failureApiResult = ApiResult.initFailure();
            failureApiResult.setMessage(e.getMessage());
            return failureApiResult;
        }
        successApiResult.setMessage("执行成功！");
        return successApiResult;
    }


}
