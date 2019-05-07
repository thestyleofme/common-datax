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
@SuppressWarnings("unused")
@Slf4j
public class SftpUtil implements AutoCloseable {

	private Session session = null;
	private ChannelSftp channelSftp = null;
	private static final int TIMEOUT = 60000;
	private static final String SPLIT_PATTERN = "/";
	private static final String SFTP_SERVER_NOT_LOGIN = "sftp server not login";


	/**
	 * 连接sftp服务器
	 *
	 * @param info 服务IP，端口，用户名，密码
	 * @throws JSchException JSchException
	 */
	public void connectServerUseSftp(String... info)
			throws JSchException {
		final String serverIP = info[0];
		final String username = info[2];
		this.getSession(serverIP, Integer.parseInt(info[1]), username, info[3]);
		// 打开SFTP通道
		channelSftp = (ChannelSftp) session.openChannel("sftp");
		// 建立SFTP通道的连接
		channelSftp.connect();
		log.info("Connected successfully to ip :{}, ftpUsername is :{}, return :{}", serverIP, username, channelSftp);
	}

	/**
	 * 连接sftp服务器
	 *
	 * @param command 执行的命令
	 * @param info    服务IP，端口，用户名，密码
	 * @throws JSchException JSchException
	 */
	public void connectServerUseExec(String command, String... info)
			throws JSchException, IOException {
		final String serverIP = info[0];
		final String username = info[2];
		this.getSession(serverIP, Integer.parseInt(info[1]), username, info[3]);
		// 打开SFTP通道
		ChannelExec channelExec = (ChannelExec) session.openChannel("exec");
		channelExec.setCommand(command);
		channelExec.setInputStream(null);
		channelExec.setErrStream(new PrintStream(new FileOutputStream(FileDescriptor.err)));
		InputStream in = channelExec.getInputStream();
		// 建立EXEC通道的连接
		channelExec.connect();
		log.info("Connected successfully to ip :{}, execUsername is :{}, return :{}", serverIP, username, channelExec);
		byte[] tmp = new byte[1024];
		while (true) {
			while (in.available() > 0) {
				int i = in.read(tmp, 0, 1024);
				if (i < 0) {
					break;
				}
				log.info(new String(tmp, 0, i));
			}
			if (channelExec.isClosed()) {
				if (in.available() > 0) {
					continue;
				}
				log.info("exit-status: " + channelExec.getExitStatus());
				break;
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				log.warn("InterruptedException,", e);
				Thread.currentThread().interrupt();
			}
		}
	}

	/**
	 * 获取session
	 *
	 * @param serverIP 服务IP
	 * @param port     端口
	 * @param username 用户名
	 * @param password 密码
	 * @throws JSchException JSchException
	 */
	private void getSession(String serverIP, int port, String username, String password) throws JSchException {
		// 创建JSch对象
		JSch jsch = new JSch();
		// 根据用户名，主机ip，端口获取一个Session对象
		session = jsch.getSession(username, serverIP, port);
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
	}

	/**
	 * 自动关闭channel和session
	 */
	@Override
	public void close() {
		if (!Objects.isNull(channelSftp)) {
			channelSftp.disconnect();
		}
		if (!Objects.isNull(session)) {
			session.disconnect();
		}
	}

	/**
	 * @param path path
	 * @return List<ChannelSftp.LsEntry>
	 * @throws SftpException s
	 * @date 2018/7/16 21:57
	 */
	public List<ChannelSftp.LsEntry> getDirList(String path) throws SftpException {
		List<ChannelSftp.LsEntry> list = new ArrayList<>();
		if (channelSftp != null) {
			Vector vv = channelSftp.ls(path);
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
	 * @date 2018/7/16 21:57
	 */
	public List<ChannelSftp.LsEntry> getFiles(String path, String suffix) throws SftpException {
		List<ChannelSftp.LsEntry> list = new ArrayList<>();
		if (channelSftp != null) {
			channelSftp.ls(path, lsEntry -> {
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
			if (Objects.isNull(channelSftp)) {
				throw new IOException(SFTP_SERVER_NOT_LOGIN);
			}
			channelSftp.get(remotePathFile, os);
		}
	}

	/**
	 * @param pathFile 远程文件
	 * @return InputStream InputStream
	 * @throws SftpException SftpException
	 */
	public InputStream getFileInputStream(String pathFile) throws SftpException {
		return channelSftp.get(pathFile);
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
			if (Objects.isNull(channelSftp)) {
				throw new IOException(SFTP_SERVER_NOT_LOGIN);
			}
			channelSftp.put(in, remoteFile);
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
		if (Objects.isNull(channelSftp)) {
			throw new IOException(SFTP_SERVER_NOT_LOGIN);
		}
		channelSftp.put(in, remoteFile);
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
		if (Objects.isNull(channelSftp)) {
			throw new IOException(SFTP_SERVER_NOT_LOGIN);
		}
		return channelSftp.lstat(filePath).getSize();
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
		if (Objects.isNull(channelSftp)) {
			throw new IOException(SFTP_SERVER_NOT_LOGIN);
		}
		channelSftp.rename(sourcePath, targetPath);
	}

	/**
	 * 删除服务器文件
	 *
	 * @param filePath filePath
	 * @throws IOException IOException
	 */
	public void remove(String filePath) throws IOException {
		if (Objects.isNull(channelSftp)) {
			throw new IOException(SFTP_SERVER_NOT_LOGIN);
		}
		try {
			channelSftp.rename(filePath, filePath + ".bak");
			channelSftp.rm(filePath);
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
		if (Objects.isNull(channelSftp)) {
			throw new IOException(SFTP_SERVER_NOT_LOGIN);
		}
		for (String d : dir.split(SPLIT_PATTERN)) {
			builder.append(SPLIT_PATTERN).append(d);
			try {
				channelSftp.ls(builder.toString());
			} catch (SftpException e) {
				channelSftp.mkdir(builder.toString());
			}
		}
	}

}
