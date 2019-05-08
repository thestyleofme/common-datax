package com.isacc.datax.infra.util;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.isacc.datax.api.dto.ApiResult;
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

    public static ApiResult<Object> checkFieldDelimiter(String fieldDelimiter) {
        final int length = fieldDelimiter.replace(Constants.Symbol.BACKSLASH, "").replace(Constants.Symbol.SLASH, "").length();
        if (length != 1) {
            final ApiResult<Object> failureApiResult = ApiResult.initFailure();
            failureApiResult.setMessage(String.format("datax supports only single-character field delimiter, which you configure as : [%s]", fieldDelimiter));
            return failureApiResult;
        }
        return ApiResult.initSuccess();
    }

    public static ApiResult<Object> checkWriteMode(String writeMode) {
        List<HdfsWriterModeEnum> writeModeInfo = Arrays.stream(HdfsWriterModeEnum.values()).filter(hdfsWriterModeEnum -> writeMode.equalsIgnoreCase(hdfsWriterModeEnum.getWriteMode())).collect(Collectors.toList());
        if (writeModeInfo.isEmpty()) {
            final ApiResult<Object> failureApiResult = ApiResult.initFailure();
            failureApiResult.setMessage("datax doesn't have this writerMode: " + writeMode);
            return failureApiResult;
        }
        return ApiResult.initSuccess();
    }

}
