package com.isacc.datax.infra.util;

import java.io.*;
import java.util.*;

import com.jcraft.jsch.*;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 * Sftp Util
 * </p>
 *
 * @author isacc 2019/04/09 16:17
 */
@SuppressWarnings({"Duplicates", "UnusedReturnValue", "unused"})
@Slf4j
public class SftpUtil implements AutoCloseable {

	private Session session = null;
	private ChannelSftp channel = null;
	private static final int TIMEOUT = 60000;
	private static final String SPLIT_PATTERN = "/";
	private static final String SFTP_SERVER_NOT_LOGIN = "sftp server not login";


	/**
	 * 连接sftp服务器
	 *
	 * @param serverIP 服务IP
	 * @param port     端口
	 * @param userName 用户名
	 * @param password 密码
	 * @throws JSchException JSchException
	 */
	public ChannelSftp connectServer(String serverIP, int port, String userName, String password)
			throws JSchException {
		// 创建JSch对象
		JSch jsch = new JSch();
		// 根据用户名，主机ip，端口获取一个Session对象
		session = jsch.getSession(userName, serverIP, port);
		log.info("Session created...");
		if (!Objects.isNull(password)) {
			// 设置密码
			session.setPassword(password);
		}
		// 为Session对象设置properties
		Properties config = new Properties();
		config.put("StrictHostKeyChecking", "no");
		session.setConfig(config);
		// 设置timeout时间
		session.setTimeout(TIMEOUT);
		// 通过Session建立连接
		session.connect();
		log.info("Session connected, Opening Channel...");
		// 打开SFTP通道
		channel = (ChannelSftp) session.openChannel("sftp");
		// 建立SFTP通道的连接
		channel.connect();
		log.info("Connected successfully to ip :{}, ftpUsername is :{}, return :{}", serverIP, userName, channel);
		return channel;
	}

	/**
	 * 自动关闭channel和session
	 */
	@Override
	public void close() {
		if (!Objects.isNull(channel)) {
			channel.disconnect();
		}
		if (!Objects.isNull(session)) {
			session.disconnect();
		}
	}

	/**
	 * @param path path
	 * @return List<ChannelSftp.LsEntry>
	 * @throws SftpException s
	 * @author zhilong.deng@hand-china.com
	 * @date 2018/7/16 21:57
	 */
	public List<ChannelSftp.LsEntry> getDirList(String path) throws SftpException {
		List<ChannelSftp.LsEntry> list = new ArrayList<>();
		if (channel != null) {
			Vector vv = channel.ls(path);
			if (vv == null || vv.isEmpty()) {
				return list;
			} else {
				Object[] aa = vv.toArray();
				for (Object obj : aa) {
					ChannelSftp.LsEntry temp = (ChannelSftp.LsEntry) obj;
					list.add(temp);
				}
			}
		}
		return list;
	}

	/**
	 * @param path   path
	 * @param suffix suffix
	 * @return List<ChannelSftp.LsEntry>
	 * @throws SftpException s
	 * @author zhilong.deng@hand-china.com
	 * @date 2018/7/16 21:57
	 */
	public List<ChannelSftp.LsEntry> getFiles(String path, String suffix) throws SftpException {
		List<ChannelSftp.LsEntry> list = new ArrayList<>();
		if (channel != null) {
			channel.ls(path, lsEntry -> {
				if (!lsEntry.getAttrs().isDir() && lsEntry.getFilename().endsWith(suffix)) {
					list.add(lsEntry);
				}
				return 0;
			});

		}
		return list;
	}

	/**
	 * 下载文件
	 *
	 * @param remotePathFile 远程文件
	 * @param localPathFile  本地文件[绝对路径]
	 * @throws SftpException SftpException
	 * @throws IOException   IOException
	 */
	public void downloadFile(String remotePathFile, String localPathFile)
			throws SftpException, IOException {
		try (FileOutputStream os = new FileOutputStream(new File(localPathFile))) {
			if (Objects.isNull(channel)) {
				throw new IOException(SFTP_SERVER_NOT_LOGIN);
			}
			channel.get(remotePathFile, os);
		}
	}

	/**
	 * @param pathFile 远程文件
	 * @return InputStream InputStream
	 * @throws SftpException SftpException
	 */
	public InputStream getFileInputStream(String pathFile) throws SftpException {
		return channel.get(pathFile);
	}

	/**
	 * 上传文件
	 *
	 * @param remoteFile 远程文件
	 * @param localFile  l
	 * @throws SftpException s
	 * @throws IOException   i
	 */
	public void uploadFileWithStr(String remoteFile, String localFile)
			throws SftpException, IOException {
		try (FileInputStream in = new FileInputStream(new File(localFile))) {
			if (Objects.isNull(channel)) {
				throw new IOException(SFTP_SERVER_NOT_LOGIN);
			}
			channel.put(in, remoteFile);
		}
	}

	/**
	 * 上传文件
	 *
	 * @param remoteFile 远程文件
	 * @param in         InputStream
	 * @throws SftpException s
	 * @throws IOException   i
	 */
	public void uploadFile(String remoteFile, InputStream in) throws SftpException, IOException {
		if (Objects.isNull(channel)) {
			throw new IOException(SFTP_SERVER_NOT_LOGIN);
		}
		channel.put(in, remoteFile);
	}

	/**
	 * 获取文件大小
	 *
	 * @param filePath filePath
	 * @return Long
	 * @throws SftpException SftpException
	 * @throws IOException   IOException
	 */
	public Long getFileSize(String filePath) throws SftpException, IOException {
		if (Objects.isNull(channel)) {
			throw new IOException(SFTP_SERVER_NOT_LOGIN);
		}
		return channel.lstat(filePath).getSize();
	}

	/**
	 * 移动文件
	 *
	 * @param sourcePath sourcePath
	 * @param targetPath targetPath
	 * @throws SftpException SftpException
	 * @throws IOException   IOException
	 */
	public void rename(String sourcePath, String targetPath) throws SftpException, IOException {
		if (Objects.isNull(channel)) {
			throw new IOException(SFTP_SERVER_NOT_LOGIN);
		}
		channel.rename(sourcePath, targetPath);
	}

	/**
	 * 删除服务器文件
	 *
	 * @param filePath filePath
	 * @throws IOException IOException
	 */
	public void remove(String filePath) throws IOException {
		if (Objects.isNull(channel)) {
			throw new IOException(SFTP_SERVER_NOT_LOGIN);
		}
		try {
			channel.rename(filePath, filePath + ".bak");
			channel.rm(filePath);
		} catch (SftpException e) {
			log.error("文件不存在", e);
		}
	}

	/**
	 * 创建目录
	 *
	 * @param path 目录
	 * @param dir  目录
	 * @throws SftpException s
	 * @throws IOException   i
	 */
	public synchronized void mkdir(String path, String dir) throws SftpException, IOException {
		StringBuilder builder = new StringBuilder();
		builder.append(path);
		if (Objects.isNull(channel)) {
			throw new IOException(SFTP_SERVER_NOT_LOGIN);
		}
		for (String d : dir.split(SPLIT_PATTERN)) {
			builder.append(SPLIT_PATTERN).append(d);
			try {
				channel.ls(builder.toString());
			} catch (SftpException e) {
				channel.mkdir(builder.toString());
			}
		}
	}

}
