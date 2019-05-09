package com.isacc.datax.infra.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.api.dto.HiveInfoDTO;
import com.isacc.datax.domain.entity.datax.HivePartition;
import com.isacc.datax.domain.entity.reader.hdfsreader.HdfsFileTypeEnum;
import com.isacc.datax.domain.entity.writer.hdfswiter.HdfsWriterModeEnum;
import com.isacc.datax.infra.constant.Constants;

/**
 * <p>
 * DataX util
 * </p>
 *
 * @author isacc 2019/05/07 17:08
 */
public class DataxUtil {

    private static Pattern compileEqual = Pattern.compile("=");
    private static Pattern compileDb = Pattern.compile(".db");

    private DataxUtil() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * 检验hdfs文件类型
     *
     * @param fileTypes Array[String]
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019/5/9 16:23
     */
    public static ApiResult<Object> checkHdfsFileType(String... fileTypes) {
        for (String fileType : fileTypes) {
            List<HdfsFileTypeEnum> fileTypeInfo = Arrays.stream(HdfsFileTypeEnum.values()).filter(hdfsFileTypeEnum -> fileType.equalsIgnoreCase(hdfsFileTypeEnum.name())).collect(Collectors.toList());
            if (fileTypeInfo.isEmpty()) {
                final ApiResult<Object> failureApiResult = ApiResult.initFailure();
                failureApiResult.setMessage("datax doesn't have this fileType: " + fileType);
                return failureApiResult;
            }
        }
        return ApiResult.initSuccess();
    }

    /**
     * 检验字段分隔符
     *
     * @param fieldDelimiter fieldDelimiter
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019/5/9 16:23
     */
    public static ApiResult<Object> checkFieldDelimiter(String fieldDelimiter) {
        final int length = fieldDelimiter.replace(Constants.Symbol.BACKSLASH, "").replace(Constants.Symbol.SLASH, "").length();
        if (length != 1) {
            final ApiResult<Object> failureApiResult = ApiResult.initFailure();
            failureApiResult.setMessage(String.format("datax supports only single-character field delimiter, which you configure as : [%s]", fieldDelimiter));
            return failureApiResult;
        }
        return ApiResult.initSuccess();
    }

    /**
     * 检验WriteMode
     *
     * @param writeMode writeMode
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019/5/9 16:22
     */
    public static ApiResult<Object> checkWriteMode(String writeMode) {
        List<HdfsWriterModeEnum> writeModeInfo = Arrays.stream(HdfsWriterModeEnum.values()).filter(hdfsWriterModeEnum -> writeMode.equalsIgnoreCase(hdfsWriterModeEnum.getWriteMode())).collect(Collectors.toList());
        if (writeModeInfo.isEmpty()) {
            final ApiResult<Object> failureApiResult = ApiResult.initFailure();
            failureApiResult.setMessage("datax doesn't have this writerMode: " + writeMode);
            return failureApiResult;
        }
        return ApiResult.initSuccess();
    }

    /**
     * 解析hdfs路径获取分区信息
     *
     * @param path hdfs路径
     * @return java.util.List<com.isacc.datax.domain.entity.datax.HivePartition>
     * @author isacc 2019/5/9 16:21
     */
    public static List<HivePartition> partitionList(String path) {
        List<HivePartition> partitionList = new ArrayList<>(3);
        // 判断path是否含有分区信息
        if (path.contains(Constants.Symbol.EQUAL)) {
            // 有分区信息
            Matcher matcher = compileEqual.matcher(path);
            HivePartition partition;
            while (matcher.find()) {
                // TODO: 默认创表这里是STRING.
                partition = HivePartition.builder().type("STRING").build();
                String before = path.substring(0, matcher.start());
                String partitionName = before.substring(before.lastIndexOf('/') + 1);
                partition.setName(partitionName);
                String after = path.substring(matcher.end());
                String partitionValue = after.substring(0, after.indexOf('/') == -1 ? after.length() : after.indexOf('/'));
                partition.setValue(partitionValue);
                partitionList.add(partition);
            }
        }
        return partitionList;
    }

    /**
     * 通过hdfs路径获取hive信息
     *
     * @param path hdfs信息
     * @return com.isacc.datax.api.dto.ApiResult<java.lang.Object>
     * @author isacc 2019/5/9 17:29
     */
    public static ApiResult<Object> getHiveInfoFromPath(String path) {
        ApiResult<Object> successApiResult = ApiResult.initSuccess();
        Matcher matcher = compileDb.matcher(path);
        String hiveDbName;
        String hiveTblName;
        if (!matcher.find()) {
            ApiResult<Object> pathApiResult = ApiResult.initFailure();
            pathApiResult.setMessage("path中找不到hive数据库信息!");
            pathApiResult.setContent(path);
            return pathApiResult;
        } else {
            String dbPath = path.substring(0, matcher.start());
            hiveDbName = dbPath.substring(dbPath.lastIndexOf(Constants.Symbol.SLASH) + 1);
            String tblPath = path.substring(matcher.end() + 1);
            hiveTblName = tblPath.substring(0, !tblPath.contains(Constants.Symbol.SLASH) ? tblPath.length() : tblPath.indexOf(Constants.Symbol.SLASH));
        }
        HiveInfoDTO hiveInfoDTO = HiveInfoDTO.builder().databaseName(hiveDbName).tableName(hiveTblName).build();
        successApiResult.setContent(hiveInfoDTO);
        return successApiResult;
    }

}
