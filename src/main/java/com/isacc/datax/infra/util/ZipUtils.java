package com.isacc.datax.infra.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import jline.internal.Log;

/**
 * <p>
 * description
 * </P>
 *
 * @author isacc 2019/05/14 17:11
 */
public class ZipUtils {

    private static final int BUFFER_SIZE = 2 * 1024;

    private ZipUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static void toZip(List<File> srcFiles, OutputStream out) {
        try (ZipOutputStream zos = new ZipOutputStream(out)) {
            for (File file : srcFiles) {
                byte[] buf = new byte[BUFFER_SIZE];
                zos.putNextEntry(new ZipEntry(file.getName()));
                int len;
                try (FileInputStream in = new FileInputStream(file)) {
                    while ((len = in.read(buf)) != -1) {
                        zos.write(buf, 0, len);
                    }
                }
            }
        } catch (IOException e) {
            Log.error("zip files fail!", e);
        }
    }

    public static String generateFileName(String name) {
        final LocalDateTime now = LocalDateTime.now();
        final String localDate = now.toLocalDate().toString();
        final String localTime = now.toLocalTime().toString().replace(':', '-').replace('.', '-');
        return name + "-" + localDate + "-" + localTime;
    }

}
