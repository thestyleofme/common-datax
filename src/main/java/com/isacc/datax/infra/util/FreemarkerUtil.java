package com.isacc.datax.infra.util;

import java.io.File;
import java.time.LocalDateTime;
import java.util.Locale;
import java.util.Map;

import com.isacc.datax.DataxApplication;
import com.isacc.datax.api.dto.ApiResult;
import com.isacc.datax.infra.config.DataxProperties;
import freemarker.template.Configuration;
import freemarker.template.Template;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.output.FileWriterWithEncoding;

/**
 * <p>
 * Freemarker Utils
 * </p>
 *
 * @author isacc 2019/05/05 14:25
 */
@Slf4j
public class FreemarkerUtil {

    private static Configuration getConfiguration(String basePackagePath) {
        Configuration cfg = new Configuration(Configuration.VERSION_2_3_28);
        cfg.setClassForTemplateLoading(DataxApplication.class, basePackagePath);
        return cfg;
    }

    private static String generateFileName(String name) {
        final LocalDateTime now = LocalDateTime.now();
        final String localDate = now.toLocalDate().toString();
        final String localTime = now.toLocalTime().toString().replace(':', '-').replace('.', '-');
        return name + "-" + localDate + "-" + localTime + ".json";
    }

    public static ApiResult<Object> createJsonFile(Map<String, Object> root, DataxProperties dataxProperties, String templateName) {
        final ApiResult<Object> successApiResult = ApiResult.initSuccess();
        final ApiResult<Object> failureApiResult = ApiResult.initFailure();
        try {
            Configuration cfg = FreemarkerUtil.getConfiguration(dataxProperties.getBasePackagePath());
            Template template = cfg.getTemplate(templateName, Locale.CHINA);
            final String jsonFileName = FreemarkerUtil.generateFileName(templateName.substring(0, templateName.lastIndexOf('.')));
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
            failureApiResult.setMessage("create json file failure!");
            failureApiResult.setContent(e.getMessage());
            return failureApiResult;
        }
    }


}
