package com.isacc.datax.infra.util;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

import com.isacc.datax.api.dto.ApiResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * <p>
 * description
 * </P>
 *
 * @author isacc 2019/05/13 11:38
 */
@SuppressWarnings("unused")
@Slf4j
public class HdfsUtil implements AutoCloseable {

    private static FileSystem fileSystem = null;

    private static void getFileSystem(String nameNode, String user) throws URISyntaxException, IOException, InterruptedException {
        /*
         * Configuration参数对象的机制：
         *    构造时，会加载jar包中的默认配置 xx-default.xml
         *    再加载 用户配置xx-site.xml  ，覆盖掉默认参数
         *    构造完成之后，还可以conf.set("p","v")，会再次覆盖用户配置文件中的参数值
         * new Configuration()会从项目的classpath中加载core-default.xml hdfs-default.xml core-site.xml hdfs-site.xml等文件
         */
        Configuration conf = new Configuration();
        /*
         * 指定本客户端上传文件到hdfs时需要保存的副本数为：2
         * conf.set("dfs.replication", "2")
         */
        // 指定本客户端上传文件到hdfs时切块的规格大小：128M
        conf.set("dfs.blocksize", "128m");
        fileSystem = FileSystem.get(new URI(nameNode), conf, user);
    }


    public static ApiResult<Object> upload(String nameNode, String user, String source, String target) {
        ApiResult<Object> successApiResult = ApiResult.initSuccess();
        try {
            HdfsUtil.getFileSystem(nameNode, user);
            fileSystem.mkdirs(new Path(target));
            fileSystem.copyFromLocalFile(new Path(source), new Path(target));
        } catch (Exception e) {
            ApiResult<Object> failureApiResult = ApiResult.initFailure();
            failureApiResult.setMessage(String.format("上传csv文件失败！%n%s", e.getMessage()));
            return failureApiResult;
        }
        return successApiResult;
    }

    @Override
    public void close() throws Exception {
        if (!Objects.isNull(fileSystem)) {
            fileSystem.close();
        }
    }
}
