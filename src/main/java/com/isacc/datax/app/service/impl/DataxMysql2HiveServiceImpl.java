package com.isacc.datax.app.service.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.stream.Collectors;

import javax.validation.constraints.NotBlank;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.HiveInfoDTO;
import com.isacc.datax.api.dto.Mysql2HiveDTO;
import com.isacc.datax.app.service.DataxMysql2HiveService;
import com.isacc.datax.app.service.HiveService;
import com.isacc.datax.domain.entity.reader.hdfsreader.HdfsFileTypeEnum;
import com.isacc.datax.domain.entity.reader.mysqlreader.MysqlReaderConnection;
import com.isacc.datax.domain.entity.writer.hdfswiter.HdfsWriterModeEnum;
import com.isacc.datax.infra.config.DataxProperties;
import com.isacc.datax.infra.constant.Constants;
import com.isacc.datax.infra.mapper.MysqlSimpleMapper;
import com.isacc.datax.infra.util.FreemarkerUtils;
import com.isacc.datax.infra.util.SftpUtil;
import com.jcraft.jsch.JSchException;
import freemarker.template.Configuration;
import freemarker.template.Template;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileItemFactory;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.commons.CommonsMultipartFile;

/**
 * <p>
 * DataX Service Impl
 * </p>
 *
 * @author isacc 2019/04/29 17:06
 */
@SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
@Service
@Slf4j
public class DataxMysql2HiveServiceImpl implements DataxMysql2HiveService {

	private final MysqlSimpleMapper mysqlSimpleMapper;
	private final HiveService hiveService;
	private final DataxProperties dataxProperties;

	@Autowired
	public DataxMysql2HiveServiceImpl(MysqlSimpleMapper mysqlSimpleMapper, HiveService hiveService, DataxProperties dataxProperties) {
		this.mysqlSimpleMapper = mysqlSimpleMapper;
		this.hiveService = hiveService;
		this.dataxProperties = dataxProperties;
	}

	@Override
	public ApiResult<Object> mysql2HiveWhere(Mysql2HiveDTO mysql2HiveDTO) {
		// 判断mysql的数据库/表是否存在，无则报错返回
		final ApiResult<Object> mysqlApiResult = this.mysqlDbAndTblIsExist(mysql2HiveDTO);
		if (!mysqlApiResult.getResult()) {
			return mysqlApiResult;
		}
		// 判断hive的数据库/表是否存在，无则创建库表
		final ApiResult<Object> hiveApiResult = this.hiveDbAndTblIsExist(mysql2HiveDTO);
		if (!hiveApiResult.getResult()) {
			return hiveApiResult;
		}
		// 校验writeMode
		final ApiResult<Object> writeModeApiResult = this.checkWriteMode(mysql2HiveDTO);
		if (!writeModeApiResult.getResult()) {
			return writeModeApiResult;
		}
		// 创建datax job json文件
		final ApiResult<Object> jsonResult = this.createJsonFile(mysql2HiveDTO);
		if (jsonResult.getResult()) {
			final File jsonFile = (File) jsonResult.getContent();
			// file转为MultipartFile
			final ApiResult<Object> file2MultiApiResult = this.file2MultipartFile(jsonFile);
			if (file2MultiApiResult.getResult()) {
				final MultipartFile multipartFile = (MultipartFile) file2MultipartFile(jsonFile).getContent();
				// 上传到datax服务器
				final ApiResult<Object> uploadFileApiResult = this.uploadFile(multipartFile, dataxProperties);
				if (uploadFileApiResult.getResult()) {
					// 远程执行python进行导数
					return execCommand(dataxProperties, String.valueOf(uploadFileApiResult.getContent()));
				} else {
					return uploadFileApiResult;
				}
			} else {
				return file2MultiApiResult;
			}
		} else {
			return jsonResult;
		}
	}

	/**
	 * 远程执行python
	 *
	 * @param dataxProperties DataxProperties
	 * @param jsonFileName    json file name
	 * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
	 * @author isacc 2019-05-07 10:06
	 */
	private ApiResult<Object> execCommand(DataxProperties dataxProperties, String jsonFileName) {
		String command =
				"source /etc/profile;" + dataxProperties.getHome() + "bin/datax.py " + dataxProperties.getUploadDicPath() + jsonFileName;
		try (SftpUtil util = new SftpUtil()) {
			util.connectServerUseExec(command, getDataxInfo(dataxProperties));
		} catch (JSchException | IOException e) {
			log.error("command execution failed,", e);
			final ApiResult<Object> failureApiResult = ApiResult.initFailure();
			failureApiResult.setMessage(e.getMessage());
			return failureApiResult;
		}
		return ApiResult.initSuccess();
	}

	/**
	 * file转为MultipartFile
	 *
	 * @param jsonFile File
	 * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
	 * @author isacc 2019-05-05 20:44
	 */
	private ApiResult<Object> file2MultipartFile(File jsonFile) {
		final ApiResult<Object> successApiResult = ApiResult.initSuccess();
		FileItemFactory factory = new DiskFileItemFactory(16, null);
		FileItem fileItem = factory.createItem(jsonFile.getName(), "text/html", true, jsonFile.getName());
		int bytesRead;
		int bytes = 8 * 1024;
		byte[] buffer = new byte[bytes];
		try (FileInputStream fis = new FileInputStream(jsonFile);
			 OutputStream os = fileItem.getOutputStream()) {
			while ((bytesRead = fis.read(buffer, 0, bytes)) != -1) {
				os.write(buffer, 0, bytesRead);
			}
			MultipartFile multipartFile = new CommonsMultipartFile(fileItem);
			successApiResult.setContent(multipartFile);
			return successApiResult;
		} catch (IOException e) {
			log.error("file to MultipartFile error,", e);
			final ApiResult<Object> failureApiResult = ApiResult.initFailure();
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
	private ApiResult<Object> uploadFile(MultipartFile file, DataxProperties dataxProperties) {
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

	/**
	 * 解析datax相关信息放入数组
	 *
	 * @param dataxProperties DataxProperties
	 * @return java.lang.String[]
	 * @author isacc 2019-05-07 10:21
	 */
	private String[] getDataxInfo(DataxProperties dataxProperties) {
		String ip = dataxProperties.getHost();
		String port = dataxProperties.getPort();
		String username = dataxProperties.getUsername();
		String password = dataxProperties.getPassword();
		return new String[]{ip, port, username, password};
	}


	/**
	 * 生成datax json文件
	 *
	 * @param mysql2HiveDTO Mysql2HiveDTO
	 * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
	 * @author isacc 2019-05-05 16:42
	 */
	private ApiResult<Object> createJsonFile(Mysql2HiveDTO mysql2HiveDTO) {
		try {
			final ApiResult<Object> successApiResult = ApiResult.initSuccess();
			final ApiResult<Object> failureApiResult = ApiResult.initFailure();
			Configuration cfg = FreemarkerUtils.getConfiguration(dataxProperties.getBasePackagePath());
			final Map<String, Object> root = generateDataModel(mysql2HiveDTO);
			final String whereTemplate = dataxProperties.getMysql2Hive().getWhereTemplate();
			Template template = cfg.getTemplate(whereTemplate, Locale.CHINA);
			final String jsonFileName = FreemarkerUtils.generateFileName(whereTemplate.substring(0, whereTemplate.lastIndexOf('.')));
			final File file = new File(dataxProperties.getLocalDicPath() + jsonFileName);
			if (!file.exists()) {
				final boolean newFile = file.createNewFile();
				if (!newFile) {
					failureApiResult.setMessage("the json file: " + jsonFileName + ", create failure");
					return failureApiResult;
				}
			}
			FileWriterWithEncoding writer = new FileWriterWithEncoding(file, "UTF-8");
			template.process(root, writer);
			writer.close();
			successApiResult.setContent(file);
			return successApiResult;
		} catch (Exception e) {
			log.error("create json file failure!", e);
			final ApiResult<Object> failureApiResult = ApiResult.initFailure();
			failureApiResult.setMessage("create json file failure!");
			failureApiResult.setContent(e.getMessage());
			return failureApiResult;
		}
	}

	/**
	 * 创建freemarker的DataModel
	 *
	 * @param mysql2HiveDTO Mysql2HiveDTO
	 * @return java.util.Map<java.lang.String, java.lang.Object>
	 * @author isacc 2019-05-05 16:04
	 */
	private Map<String, Object> generateDataModel(Mysql2HiveDTO mysql2HiveDTO) {
		final HashMap<String, Object> root = new HashMap<>(16);
		// setting
		root.put("setting", mysql2HiveDTO.getSetting());
		// mysql
		root.put("username", mysql2HiveDTO.getReader().getUsername());
		root.put("password", mysql2HiveDTO.getReader().getPassword());
		root.put("mysqlColumn", mysql2HiveDTO.getReader().getColumn());
		root.put("connection", mysql2HiveDTO.getReader().getConnection());
		root.put("where", mysql2HiveDTO.getReader().getWhere());
		// hdfs
		root.put("hdfsColumn", mysql2HiveDTO.getWriter().getColumn());
		root.put("defaultFS", mysql2HiveDTO.getWriter().getDefaultFS());
		root.put("fileType", mysql2HiveDTO.getWriter().getFileType());
		root.put("path", mysql2HiveDTO.getWriter().getPath());
		root.put("fileName", mysql2HiveDTO.getWriter().getFileName());
		root.put("writeMode", mysql2HiveDTO.getWriter().getWriteMode());
		root.put("fieldDelimiter", mysql2HiveDTO.getWriter().getFieldDelimiter());
		return root;
	}

	/**
	 * 检验writeMode
	 *
	 * @param mysql2HiveDTO Mysql2HiveDTO
	 * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
	 * @author isacc 2019-05-02 3:44
	 */
	private ApiResult<Object> checkWriteMode(Mysql2HiveDTO mysql2HiveDTO) {
		@NotBlank String writerMode = mysql2HiveDTO.getWriter().getWriteMode();
		List<HdfsWriterModeEnum> writerModeInfo = Arrays.stream(HdfsWriterModeEnum.values()).filter(hdfsWriterModeEnum -> writerMode.equalsIgnoreCase(hdfsWriterModeEnum.getWriteMode())).collect(Collectors.toList());
		if (writerModeInfo.isEmpty()) {
			final ApiResult<Object> failureApiResult = ApiResult.initFailure();
			failureApiResult.setMessage("datax doesn't have this writerMode: " + writerMode);
			return failureApiResult;
		}
		return ApiResult.initSuccess();
	}

	/**
	 * 创建hive数据表
	 *
	 * @param mysql2HiveDTO Mysql2HiveDTO
	 * @param hiveDb        hive数据库
	 * @param hiveTable     hive表
	 * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
	 * @author isacc 2019-05-02 4:31
	 */
	private ApiResult<Object> createHiveTable(Mysql2HiveDTO mysql2HiveDTO, String hiveDb, String hiveTable) {
		if (!this.checkHiveTableInfo(mysql2HiveDTO).getResult()) {
			return this.checkHiveTableInfo(mysql2HiveDTO);
		}
		try {
			hiveService.createTable(HiveInfoDTO.builder().
					databaseName(hiveDb).
					tableName(hiveTable).
					columns(mysql2HiveDTO.getWriter().getColumn())
					.fieldDelimiter(mysql2HiveDTO.getWriter().getFieldDelimiter())
					.fileType(HdfsFileTypeEnum.valueOf(mysql2HiveDTO.getWriter().getFileType().toUpperCase()).getFileType())
					.build());
			log.info("create hive table:{}.{}", hiveDb, hiveTable);
			return ApiResult.initSuccess();
		} catch (Exception e) {
			final ApiResult<Object> failureApiResult = ApiResult.initFailure();
			failureApiResult.setMessage("there are something went wrong in hive!");
			failureApiResult.setContent("error: " + e.getMessage());
			return failureApiResult;
		}
	}

	/**
	 * 检验fileType fieldDelimiter(单字符)
	 *
	 * @param mysql2HiveDTO Mysql2HiveDTO
	 * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
	 * @author isacc 2019-05-02 3:44
	 */
	private ApiResult<Object> checkHiveTableInfo(Mysql2HiveDTO mysql2HiveDTO) {
		@NotBlank String fileType = mysql2HiveDTO.getWriter().getFileType();
		List<HdfsFileTypeEnum> fileTypeInfo = Arrays.stream(HdfsFileTypeEnum.values()).filter(hdfsFileTypeEnum -> fileType.equalsIgnoreCase(hdfsFileTypeEnum.name())).collect(Collectors.toList());
		if (fileTypeInfo.isEmpty()) {
			final ApiResult<Object> apiResult = ApiResult.initFailure();
			apiResult.setMessage("datax doesn't have this fileType: " + fileType);
			return apiResult;
		}
		@NotBlank String fieldDelimiter = mysql2HiveDTO.getWriter().getFieldDelimiter();
		if (fieldDelimiter.replace(Constants.Symbol.BACKSLASH, "").replace(Constants.Symbol.SLASH, "").length() != 1) {
			final ApiResult<Object> apiResult = ApiResult.initFailure();
			apiResult.setMessage(String.format("datax supports only single-character field delimiter, which you configure as : [%s]", fieldDelimiter));
			return apiResult;
		}
		return ApiResult.initSuccess();
	}

	/**
	 * 校验hive的数据库、表是否存在
	 *
	 * @param mysql2HiveDTO Mysql2HiveDTO
	 * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
	 * @author isacc 2019-05-02 2:25
	 */
	private ApiResult<Object> hiveDbAndTblIsExist(Mysql2HiveDTO mysql2HiveDTO) {
		@NotBlank String path = mysql2HiveDTO.getWriter().getPath();
		String hivePath = path.substring(0, path.lastIndexOf('/'));
		String hiveDbName = hivePath.substring(hivePath.lastIndexOf('/') + 1, hivePath.indexOf('.'));
		String hiveTblName = path.substring(path.lastIndexOf('/') + 1, path.lastIndexOf('?') == -1 ? path.length() : path.lastIndexOf('?'));
		final Map<String, Object> hiveDbInfoMap = mysqlSimpleMapper.hiveDbIsExist(hiveDbName);
		if (Objects.isNull(hiveDbInfoMap)) {
			// 不存在hive数据库，先创建库，再根据所选字段创建表
			hiveService.createDatabase(hiveDbName);
			log.info("create hive database：{}", hiveDbName);
			return this.createHiveTable(mysql2HiveDTO, hiveDbName, hiveTblName);
		} else {
			final Long dbId = Long.valueOf(String.valueOf(hiveDbInfoMap.get("DB_ID")));
			final Map<String, Object> hiveTblInfoMap = mysqlSimpleMapper.hiveTblIsExist(dbId, hiveTblName);
			if (Objects.isNull(hiveTblInfoMap)) {
				// 存在hive数据库但不存在表，根据所选字段创建表
				return this.createHiveTable(mysql2HiveDTO, hiveDbName, hiveTblName);
			}
			return ApiResult.initSuccess();
		}
	}

	/**
	 * 校验mysql的数据库、表是否存在
	 *
	 * @param mysql2HiveDTO Mysql2HiveDTO
	 * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
	 * @author isacc 2019-04-29 21:11
	 */
	private ApiResult<Object> mysqlDbAndTblIsExist(Mysql2HiveDTO mysql2HiveDTO) {
		final ApiResult<Object> failureApiResult = ApiResult.initFailure();
		final List<String> databaseNameList = new ArrayList<>(5);
		final List<String> tableList = new ArrayList<>(10);
		mysql2HiveDTO.getReader().getConnection().stream().map(MysqlReaderConnection::getJdbcUrl).forEach(jdbcUrls -> jdbcUrls.forEach(url ->
				databaseNameList.add(url.substring(url.lastIndexOf('/') + 1, !url.contains("?") ? url.length() : url.indexOf('?')))
		));
		List<String> collect = databaseNameList.stream().distinct().collect(Collectors.toList());
		if (collect.size() != 1) {
			failureApiResult.setMessage("mysqlreader jdbcUrl database has too many!");
			failureApiResult.setContent("databases: " + collect);
			return failureApiResult;
		}
		String databaseName = collect.get(0);
		if (mysqlSimpleMapper.mysqlDbIsExist(databaseName) == 0) {
			failureApiResult.setMessage("mysqlreader jdbcUrl database is not exist!");
			failureApiResult.setContent("database: " + databaseName);
			return failureApiResult;
		}
		mysql2HiveDTO.getReader().getConnection().stream().map(MysqlReaderConnection::getTable).forEach(tableList::addAll);
		final List<String> notExistTables = new ArrayList<>(10);
		tableList.forEach(table -> {
			if (mysqlSimpleMapper.mysqlTblIsExist(databaseName, table) == 0) {
				notExistTables.add(table);
			}
		});
		if (!notExistTables.isEmpty()) {
			failureApiResult.setMessage("mysqlreader table does not exist!");
			failureApiResult.setContent("table not exist: " + notExistTables);
			return failureApiResult;
		}
		return ApiResult.initSuccess();
	}


}
