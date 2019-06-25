package com.isacc.datax.app.service.impl;

import java.io.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.DataxSyncDTO;
import com.isacc.datax.app.service.BaseDataxService;
import com.isacc.datax.infra.config.AzkabanProperties;
import com.isacc.datax.infra.config.DataxProperties;
import com.isacc.datax.infra.util.DataxUtil;
import com.isacc.datax.infra.util.FreemarkerUtil;
import com.isacc.datax.infra.util.SftpUtil;
import com.isacc.datax.infra.util.ZipUtil;
import com.jcraft.jsch.JSchException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileItemFactory;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.commons.CommonsMultipartFile;

/**
 * <p>
 * description
 * </P>
 *
 * @author isacc 2019/05/18 0:50
 */
@Slf4j
public class BaseDataxServiceImpl implements BaseDataxService {

    @Override
    public ApiResult<Object> generateJsonFileAndUpload(Map<String, Object> dataModel, String template, String jsonFileName, DataxProperties dataxProperties) {
        // 使用freemarker创建datax job json文件
        final ApiResult<Object> jsonResult = FreemarkerUtil.createJsonFile(dataModel, dataxProperties, template, jsonFileName);
        if (!jsonResult.getResult()) {
            return jsonResult;
        }
        final File jsonFile = (File) jsonResult.getContent();
        // file转为MultipartFile
        final ApiResult<Object> file2MultiApiResult = this.file2MultipartFile(jsonFile);
        if (!file2MultiApiResult.getResult()) {
            return file2MultiApiResult;
        }
        final MultipartFile multipartFile = (MultipartFile) file2MultipartFile(jsonFile).getContent();
        // 上传到datax服务器
        return this.uploadFile(multipartFile, dataxProperties);
    }

    @Override
    public ApiResult<Object> generateAzkabanZip(String jsonFileName, AzkabanProperties azkabanProperties, DataxProperties dataxProperties) {
        ApiResult<Object> successResult = ApiResult.initSuccess();
        ApiResult<Object> failureResult = ApiResult.initFailure();
        // 创建任务 这里先一次性执行，使用azkaban调度运行
        String dataxParamProperties = azkabanProperties.getLocalDicPath() + azkabanProperties.getDataxProperties();
        File propertiesFile = new File(dataxParamProperties);
        try {
            // 生成properties文件
            FileUtils.touch(propertiesFile);
        } catch (IOException e) {
            log.error("generate azkaban properties fail,", e);
            failureResult.setMessage("generate azkaban properties fail" + e.getMessage());
            return failureResult;
        }
        // 生成dataxParams.properties
        Properties properties = new Properties();
        try (FileOutputStream fos = new FileOutputStream(dataxParamProperties)) {
            properties.setProperty("DATAX_HOME", dataxProperties.getHome());
            properties.setProperty("DATAX_JSON_FILE_NAME", jsonFileName);
            properties.store(fos, "datax properties");
        } catch (IOException e) {
            log.error("dataxParams.properties generate error！", e);
            failureResult.setMessage("IOException: " + e.getMessage());
            return failureResult;
        }
        // 压缩dataxParams.properties和json file
        ArrayList<File> files = new ArrayList<>();
        files.add(propertiesFile);
        File generateFile = this.generateNewFile(azkabanProperties.getDataxJob(), new File(azkabanProperties.getLocalDicPath() + azkabanProperties.getDataxJob()));
        files.add(generateFile);
        files.add(FileUtils.getFile(dataxProperties.getLocalDicPath(), jsonFileName));
        String zipName = jsonFileName.substring(0, jsonFileName.indexOf('.'));
        String zipPath = azkabanProperties.getLocalDicPath() + zipName + ".zip";
        try (FileOutputStream zipOut = new FileOutputStream(zipPath)) {
            ZipUtil.toZip(files, zipOut);
            successResult.setContent(zipName);
            // 压缩过后删除dataxParams.properties
            FileUtils.deleteQuietly(propertiesFile);
        } catch (IOException e) {
            log.error("dataxJob.zip generate error！", e);
            failureResult.setMessage("IOException: " + e.getMessage());
            return failureResult;
        }
        return successResult;
    }

    @Override
    public ApiResult<Object> writeDataxSettingInfo(DataxSyncDTO dataxSyncDTO, DataxProperties dataxProperties, AzkabanProperties azkabanProperties) {
        ApiResult<Object> successResult = ApiResult.initSuccess();
        ApiResult<Object> failureResult = ApiResult.initFailure();
        try {
            String jsonFilePath = dataxProperties.getLocalDicPath() + dataxSyncDTO.getJsonFileName();
            dataxSyncDTO.setSettingInfo(ArrayUtils.toObject(FileUtils.readFileToByteArray(new File(jsonFilePath))));
            // 删除本地zip jsonFile
            FileUtils.forceDelete(new File(dataxProperties.getLocalDicPath()));
            FileUtils.forceDelete(new File(azkabanProperties.getLocalDicPath()));
        } catch (IOException e) {
            log.error("readFileToByteArray error！", e);
            failureResult.setMessage("readFileToByteArray error: " + e.getMessage());
            return failureResult;
        }
        successResult.setContent(dataxSyncDTO);
        return successResult;
    }

    @Override
    public ApiResult<Object> deleteDataxJsonFile(DataxProperties dataxProperties, DataxSyncDTO dataxSyncDTO) {
        ApiResult<Object> successResult = ApiResult.initSuccess();
        try (SftpUtil sftpUtil = new SftpUtil()) {
            sftpUtil.connectServerUseSftp(getDataxInfo(dataxProperties));
            sftpUtil.remove(dataxProperties.getUploadDicPath() + dataxSyncDTO.getJsonFileName(), false);
        } catch (IOException | JSchException e) {
            log.error("delete datax json file error,", e);
            ApiResult<Object> failureResult = ApiResult.initFailure();
            failureResult.setMessage("delete datax json file error," + e.getMessage());
            return failureResult;
        }
        return successResult;
    }

    /**
     * 获取绝对路径
     *
     * @param path resources下文件或文件夹路径
     * @param file 文件
     * @return java.lang.String
     * @author isacc 2019/5/21 12:47
     */
    private File generateNewFile(String path, File file) {
        try {
            InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
            if (resourceAsStream != null) {
                FileUtils.copyInputStreamToFile(resourceAsStream, file);
            }
        } catch (IOException e) {
            log.error("path is error,", e);
        }
        return file;
    }

    /**
     * File转为MultipartFile
     *
     * @param file File
     * @return com.hand.hdsp.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019/5/18 1:03
     */
    private ApiResult<Object> file2MultipartFile(File file) {
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
     * @param dataxProperties datax相关信息
     * @return com.hand.hdsp.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019/5/18 1:05
     */
    private ApiResult<Object> uploadFile(MultipartFile file, DataxProperties dataxProperties) {
        final ApiResult<Object> failureApiResult = ApiResult.initFailure();
        final ApiResult<Object> successApiResult = ApiResult.initSuccess();
        if (file.isEmpty()) {
            failureApiResult.setMessage("the select file is empty!");
            return failureApiResult;
        }
        String fileName = file.getOriginalFilename();
        String uploadDicPath = dataxProperties.getUploadDicPath();
        try (SftpUtil util = new SftpUtil()) {
            util.connectServerUseSftp(getDataxInfo(dataxProperties));
            util.uploadFile(uploadDicPath + '/' + fileName, file.getInputStream());
        } catch (Exception e) {
            log.error("upload json file error,", e);
            failureApiResult.setMessage("upload file error!");
            failureApiResult.setContent(e.getMessage());
            return failureApiResult;
        }
        return successApiResult;
    }

    /**
     * 生成连接sftp参数数组
     *
     * @param dataxProperties datax信息
     * @return java.lang.String[]
     * @author isacc 2019/5/18 1:05
     */
    private String[] getDataxInfo(DataxProperties dataxProperties) {
        String ip = dataxProperties.getHost();
        String port = dataxProperties.getPort();
        String username = dataxProperties.getUsername();
        String password = dataxProperties.getPassword();
        return new String[]{ip, port, username, password};
    }

    /**
     * 检验hdfsreader/hdfswriter中fileType，fieldDelimiter，writeMode参数
     *
     * @param fileTypes      fileTypes
     * @param fieldDelimiter fieldDelimiter
     * @param writeMode      writeMode
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019-05-08 9:29
     */
    ApiResult<Object> checkHdfsParams(String[] fileTypes, String fieldDelimiter, String writeMode) {
        // fileType
        final ApiResult<Object> checkReaderHdfsFileTypeApiResult = DataxUtil.checkHdfsFileType(fileTypes);
        if (!checkReaderHdfsFileTypeApiResult.getResult()) {
            return checkReaderHdfsFileTypeApiResult;
        }
        // fieldDelimiter
        final ApiResult<Object> checkReaderFieldDelimiterApiResult = DataxUtil.checkFieldDelimiter(fieldDelimiter);
        if (!checkReaderFieldDelimiterApiResult.getResult()) {
            return checkReaderFieldDelimiterApiResult;
        }
        // writeMode
        final ApiResult<Object> writeModeApiResult = DataxUtil.checkWriteMode(writeMode);
        if (!writeModeApiResult.getResult()) {
            return writeModeApiResult;
        }
        return ApiResult.initSuccess();
    }

}
