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

	public static Configuration getConfiguration(){
		Configuration cfg = new Configuration(Configuration.VERSION_2_3_28);
		cfg.setClassForTemplateLoading(DataxApplication.class, "/templates");
		return cfg;
	}

	public static String generateFileName(){
		final LocalDateTime now = LocalDateTime.now();
		final String localDate = now.toLocalDate().toString();
		final String localTime = now.toLocalTime().toString().replace(':', '-').replace('.', '-');
		return "mysql2hive_where" + localDate + "-" + localTime + ".json";
	}


}
