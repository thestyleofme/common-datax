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
import com.isacc.datax.app.service.DataxService;
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
public class DataxServiceImpl implements DataxService {

	private final MysqlSimpleMapper mysqlSimpleMapper;
	private final HiveService hiveService;
	private final DataxProperties dataxProperties;

	@Autowired
	public DataxServiceImpl(MysqlSimpleMapper mysqlSimpleMapper, HiveService hiveService, DataxProperties dataxProperties) {
		this.mysqlSimpleMapper = mysqlSimpleMapper;
		this.hiveService = hiveService;
		this.dataxProperties = dataxProperties;
	}

	@Override
	public ApiResult<Object> mysql2Hive(Mysql2HiveDTO mysql2HiveDTO) {
		// 判断mysql的数据库/表是否存在，无则报错返回
		if (!this.mysqlDbAndTblIsExist(mysql2HiveDTO).getResult()) {
			return this.mysqlDbAndTblIsExist(mysql2HiveDTO);
		}
		// 判断hive的数据库/表是否存在，无则创建库表
		if (!this.hiveDbAndTblIsExist(mysql2HiveDTO).getResult()) {
			return this.hiveDbAndTblIsExist(mysql2HiveDTO);
		}
		// 校验writeMode
		if (!this.checkWriteMode(mysql2HiveDTO).getResult()) {
			return this.checkWriteMode(mysql2HiveDTO);
		}
		// 创建datax job json文件
		if (this.createJsonFile(mysql2HiveDTO).getResult()) {
			final ApiResult<Object> jsonResult = this.createJsonFile(mysql2HiveDTO);
			final File jsonFile = (File) jsonResult.getContent();
			// file转为MultipartFile
			if (this.file2MultipartFile(jsonFile).getResult()) {
				final MultipartFile multipartFile = (MultipartFile) file2MultipartFile(jsonFile).getContent();
				// 上传到datax服务器
				if (this.uploadFile(multipartFile, dataxProperties).getResult()) {
					// 远程执行python进行导数
					return ApiResult.initSuccess();
				} else {
					return this.uploadFile(multipartFile, dataxProperties);
				}
			} else {
				return this.file2MultipartFile(jsonFile);
			}
		} else {
			return this.createJsonFile(mysql2HiveDTO);
		}
	}

	/**
	 * file转为MultipartFile
	 *
	 * @param jsonFile File
	 * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
	 * @author isacc 2019-05-05 20:44
	 */
	private ApiResult<Object> file2MultipartFile(File jsonFile) {
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
			ApiResult.SUCCESS.setContent(multipartFile);
			return ApiResult.SUCCESS;
		} catch (IOException e) {
			log.error("upload json file error,", e);
			ApiResult.FAILURE.setResult(false);
			ApiResult.FAILURE.setMessage(e.getMessage());
			return ApiResult.FAILURE;
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
		if (file.isEmpty()) {
			ApiResult.FAILURE.setResult(false);
			ApiResult.FAILURE.setMessage("the select file is empty!");
			return ApiResult.FAILURE;
		}
		String fileName = file.getOriginalFilename();
		try (SftpUtil util = new SftpUtil()) {
			this.connectServer(dataxProperties, util);
			util.uploadFile(dataxProperties.getUploadDicPath() + '/' + fileName, file.getInputStream());
		} catch (Exception e) {
			log.error("upload json file error,", e);
			ApiResult.FAILURE.setResult(false);
			ApiResult.FAILURE.setMessage("upload file error!");
			ApiResult.FAILURE.setContent(e.getMessage());
			return ApiResult.FAILURE;
		}
		return ApiResult.SUCCESS;
	}

	/**
	 * 连接到datax服务器
	 *
	 * @param dataxProperties DataxProperties
	 * @param util            SftpUtil
	 * @author isacc 2019-05-05 20:02
	 */
	private void connectServer(DataxProperties dataxProperties, SftpUtil util) throws JSchException {
		String userName = dataxProperties.getUsername();
		String ip = dataxProperties.getHost();
		int port = Integer.parseInt(dataxProperties.getPort());
		String password = dataxProperties.getPassword();
		util.connectServer(ip, port, userName, password);
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
			Configuration cfg = FreemarkerUtils.getConfiguration(dataxProperties.getBasePackagePath());
			final Map<String, Object> root = generateDataModel(mysql2HiveDTO);
			final String whereTemplate = dataxProperties.getMysql2Hive().getWhereTemplate();
			Template template = cfg.getTemplate(whereTemplate, Locale.CHINA);
			final String jsonFileName = FreemarkerUtils.generateFileName(whereTemplate.substring(0, whereTemplate.lastIndexOf('.')));
			final File file = new File(dataxProperties.getLocalDicPath() + jsonFileName);
			if (!file.exists()) {
				final boolean newFile = file.createNewFile();
				if (!newFile) {
					ApiResult.FAILURE.setMessage("the json file: " + jsonFileName + ", create failure");
					return ApiResult.FAILURE;
				}
			}
			FileWriterWithEncoding writer = new FileWriterWithEncoding(file, "UTF-8");
			template.process(root, writer);
			writer.close();
			ApiResult.SUCCESS.setContent(file);
			return ApiResult.SUCCESS;
		} catch (Exception e) {
			log.error("create json file failure!", e);
			ApiResult.FAILURE.setMessage("create json file failure!");
			ApiResult.FAILURE.setContent(e.getMessage());
			return ApiResult.FAILURE;
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
			ApiResult.FAILURE.setMessage("datax doesn't have this writerMode: " + writerMode);
			return ApiResult.FAILURE;
		}
		return ApiResult.SUCCESS;
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
			return ApiResult.SUCCESS;
		} catch (Exception e) {
			ApiResult.FAILURE.setMessage("there are something went wrong in hive!");
			ApiResult.FAILURE.setContent("error: " + e.getMessage());
			return ApiResult.FAILURE;
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
			ApiResult.FAILURE.setMessage("datax doesn't have this fileType: " + fileType);
			return ApiResult.FAILURE;
		}
		@NotBlank String fieldDelimiter = mysql2HiveDTO.getWriter().getFieldDelimiter();
		if (fieldDelimiter.replace(Constants.Symbol.BACKSLASH, "").replace(Constants.Symbol.SLASH, "").length() != 1) {
			ApiResult.FAILURE.setMessage(String.format("datax supports only single-character field delimiter, which you configure as : [%s]", fieldDelimiter));
			return ApiResult.FAILURE;
		}
		return ApiResult.SUCCESS;
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
		@NotBlank String defaultFS = mysql2HiveDTO.getWriter().getDefaultFS();
		String hiveUrl = path.substring(0, path.lastIndexOf('/'));
		String hiveDb = hiveUrl.substring(hiveUrl.lastIndexOf('/') + 1, hiveUrl.indexOf('.'));
		String hiveTable = path.substring(path.lastIndexOf('/') + 1, path.lastIndexOf('?') == -1 ? path.length() : path.lastIndexOf('?'));
		List<Map<String, Object>> allHiveDatabases = mysqlSimpleMapper.allHiveDatabases();
		List<Map<String, Object>> databaseInfo = allHiveDatabases.stream().filter(map -> String.valueOf(map.get("DB_LOCATION_URI")).equals(defaultFS + hiveUrl)).collect(Collectors.toList());
		if (databaseInfo.isEmpty()) {
			// 不存在hive数据库，先创建库，再根据所选字段创建表
			hiveService.createDatabase(hiveDb);
			log.info("create hive database：{}", hiveDb);
			return this.createHiveTable(mysql2HiveDTO, hiveDb, hiveTable);
		} else {
			List<Map<String, Object>> allHiveTblsInDb = mysqlSimpleMapper.allHiveTableByDatabase(Long.valueOf(String.valueOf(databaseInfo.get(0).get("DB_ID"))));
			List<Map<String, Object>> hiveTableInfo = allHiveTblsInDb.stream().filter(map -> String.valueOf(map.get("TBL_NAME")).equals(hiveTable)).collect(Collectors.toList());
			if (hiveTableInfo.isEmpty()) {
				// 存在hive数据库但不存在表，根据所选字段创建表
				return this.createHiveTable(mysql2HiveDTO, hiveDb, hiveTable);
			}
			return ApiResult.SUCCESS;
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
		final List<String> databaseNameList = new ArrayList<>(5);
		final List<String> tableList = new ArrayList<>(10);
		mysql2HiveDTO.getReader().getConnection().stream().map(MysqlReaderConnection::getJdbcUrl).forEach(jdbcUrls -> jdbcUrls.forEach(url ->
				databaseNameList.add(url.substring(url.lastIndexOf('/') + 1, !url.contains("?") ? url.length() : url.indexOf('?')))
		));
		List<String> collect = databaseNameList.stream().distinct().collect(Collectors.toList());
		if (collect.size() != 1) {
			ApiResult.FAILURE.setMessage("mysqlreader jdbcUrl database has too many!");
			ApiResult.FAILURE.setContent("databases: " + collect);
			return ApiResult.FAILURE;
		}
		String databaseName = collect.get(0);
		if (mysqlSimpleMapper.databaseIsExist(databaseName) == 0) {
			ApiResult.FAILURE.setMessage("mysqlreader jdbcUrl database is not exist!");
			ApiResult.FAILURE.setContent("database: " + databaseName);
			return ApiResult.FAILURE;
		}
		mysql2HiveDTO.getReader().getConnection().stream().map(MysqlReaderConnection::getTable).forEach(tableList::addAll);
		final List<String> notExistTables = new ArrayList<>(10);
		tableList.forEach(table -> {
			if (mysqlSimpleMapper.tableIsExist(databaseName, table) == 0) {
				notExistTables.add(table);
			}
		});
		if (!notExistTables.isEmpty()) {
			ApiResult.FAILURE.setMessage("mysqlreader table does not exist!");
			ApiResult.FAILURE.setContent("table not exist: " + notExistTables);
			return ApiResult.FAILURE;
		}
		return ApiResult.SUCCESS;
	}


}
