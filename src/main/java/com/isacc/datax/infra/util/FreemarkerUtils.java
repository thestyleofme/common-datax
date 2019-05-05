package com.isacc.datax.infra.util;

import java.time.LocalDateTime;

import com.isacc.datax.DataxApplication;
import freemarker.template.Configuration;

/**
 * <p>
 * Freemarker Utils
 * </p>
 * 
 * @author isacc 2019/05/05 14:25
 */
public class FreemarkerUtils {

	public static Configuration getConfiguration(String basePackagePath){
		Configuration cfg = new Configuration(Configuration.VERSION_2_3_28);
		cfg.setClassForTemplateLoading(DataxApplication.class, basePackagePath);
		return cfg;
	}

	public static String generateFileName(String name){
		final LocalDateTime now = LocalDateTime.now();
		final String localDate = now.toLocalDate().toString();
		final String localTime = now.toLocalTime().toString().replace(':', '-').replace('.', '-');
		return name + localDate + "-" + localTime + ".json";
	}


}
