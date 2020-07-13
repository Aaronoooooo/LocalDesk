package com.sshclient;

/**
 * @author pengfeisu
 * @create 2020-05-20 15:30
 */

import com.jcraft.jsch.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;

public class JSchExecutor {
    private static Logger log = LoggerFactory.getLogger(JSchExecutor.class);

    private String charset = "UTF-8";
    private String user;
    private String passwd;
    private String host;
    private int port = 22025;
    private JSch jsch;
    private Session session;

    private ChannelSftp sftp;

    /**
     * @param user   用户名
     * @param passwd 密码
     * @param host   主机IP
     */
    public JSchExecutor(String user, String passwd, String host) {
        this.user = user;
        this.passwd = passwd;
        this.host = host;
    }

    /**
     * @param user   用户名
     * @param passwd 密码
     * @param host   主机IP
     */
    public JSchExecutor(String user, String passwd, String host, int port) {
        this.user = user;
        this.passwd = passwd;
        this.host = host;
        this.port = port;
    }

    /**
     * 连接到指定的IP
     *
     * @throws JSchException
     */
    public void connect() throws JSchException {
        jsch = new JSch();
        session = jsch.getSession(user, host, port);
        session.setPassword(passwd);
        session.setConfig("StrictHostKeyChecking", "no");
        session.connect();
        Channel channel = session.openChannel("sftp");
        channel.connect();
        sftp = (ChannelSftp) channel;
        log.info("连接到SFTP成功。host: " + host);
    }

    /**
     * 关闭连接
     */
    public void disconnect() {
        if (sftp != null && sftp.isConnected()) {
            sftp.disconnect();
        }
        if (session != null && session.isConnected()) {
            session.disconnect();
        }
    }

    /**
     * 执行一条命令
     */
    public int execCmd(String command) throws Exception {
        log.info("开始执行命令:" + command);
        int returnCode = -1;
        BufferedReader reader = null;
        Channel channel = null;

        channel = session.openChannel("exec");
        ((ChannelExec) channel).setCommand(command);
        channel.setInputStream(null);
        ((ChannelExec) channel).setErrStream(System.err);
        InputStream in = channel.getInputStream();
        reader = new BufferedReader(new InputStreamReader(in));//中文乱码貌似这里不能控制，看连接的服务器的

        channel.connect();
        System.out.println("The remote command is: " + command);
        String buf;
        while ((buf = reader.readLine()) != null) {
            log.info(buf);
        }
        reader.close();
        // Get the return code only after the channel is closed.
        if (channel.isClosed()) {
            returnCode = channel.getExitStatus();
        }
        log.info("Exit-status:" + returnCode);
        channel.disconnect();
        return returnCode;
    }

    /**
     * 执行相关的命令
     */
    public void execCmd() {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        String command = "";
        BufferedReader reader = null;
        Channel channel = null;

        try {
            while ((command = br.readLine()) != null) {
                channel = session.openChannel("exec");
                ((ChannelExec) channel).setCommand(command);
                channel.setInputStream(null);
                ((ChannelExec) channel).setErrStream(System.err);

                channel.connect();
                InputStream in = channel.getInputStream();
                reader = new BufferedReader(new InputStreamReader(in, Charset.forName(charset)));
                String buf = null;
                while ((buf = reader.readLine()) != null) {
                    System.out.println(buf);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSchException e) {
            e.printStackTrace();
        } finally {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            channel.disconnect();
        }
    }

    /**
     * 上传文件
     */
    public void uploadFile(String local, String remote) throws Exception {
        File file = new File(local);
        if (file.isDirectory()) {
            throw new RuntimeException(local + "  is not a file");
        }

        InputStream inputStream = null;
        try {
            String rpath = remote.substring(0, remote.lastIndexOf("/") + 1);
            if (!isDirExist(rpath)) {
                createDir(rpath);
            }
            inputStream = new FileInputStream(file);
            sftp.setInputStream(inputStream);
            sftp.put(inputStream, remote);
        } catch (Exception e) {
            throw e;
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }

    /**
     * 下载文件
     */
    public void downloadFile(String remote, String local) throws Exception {
        OutputStream outputStream = null;
        try {
            sftp.connect(5000);
            outputStream = new FileOutputStream(new File(local));
            sftp.get(remote, outputStream);
            outputStream.flush();
        } catch (Exception e) {
            throw e;
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
    }

    /**
     * 移动到相应的目录下
     *
     * @param pathName 要移动的目录
     * @return
     */
    public boolean changeDir(String pathName) {
        if (pathName == null || pathName.trim().equals("")) {
            log.debug("invalid pathName");
            return false;
        }
        try {
            sftp.cd(pathName.replaceAll("\\\\", "/"));
            log.debug("directory successfully changed,current dir=" + sftp.pwd());
            return true;
        } catch (SftpException e) {
            log.error("failed to change directory", e);
            return false;
        }
    }

    /**
     * 创建一个文件目录，mkdir每次只能创建一个文件目录
     * 或者可以使用命令mkdir -p 来创建多个文件目录
     */
    public void createDir(String createpath) {
        try {
            if (isDirExist(createpath)) {
                sftp.cd(createpath);
                return;
            }
            String pathArry[] = createpath.split("/");
            StringBuffer filePath = new StringBuffer("/");
            for (String path : pathArry) {
                if (path.equals("")) {
                    continue;
                }
                filePath.append(path + "/");
                if (isDirExist(filePath.toString())) {
                    sftp.cd(filePath.toString());
                } else {
                    // 建立目录
                    sftp.mkdir(filePath.toString());
                    // 进入并设置为当前目录
                    sftp.cd(filePath.toString());
                }
            }
            sftp.cd(createpath);
        } catch (SftpException e) {
            throw new RuntimeException("创建路径错误：" + createpath);
        }
    }


    /**
     * 判断目录是否存在
     *
     * @param directory
     * @return
     */
    public boolean isDirExist(String directory) {
        boolean isDirExistFlag = false;
        try {
            SftpATTRS sftpATTRS = sftp.lstat(directory);
            isDirExistFlag = true;
            return sftpATTRS.isDir();
        } catch (Exception e) {
            if (e.getMessage().toLowerCase().equals("no such file")) {
                isDirExistFlag = false;
            }
        }
        return isDirExistFlag;
    }

    /**
     * 实时打印日志信息
     */
    public int shellCmd(String command) throws Exception {
        log.info("开始执行命令:" + command);
        int returnCode = -1;
        ChannelShell channel = (ChannelShell) session.openChannel("shell");
        InputStream in = channel.getInputStream();
        channel.setPty(true);
        channel.connect();
        OutputStream os = channel.getOutputStream();
        os.write((command + "\r\n").getBytes());
        os.write("exit\r\n".getBytes());
        os.flush();
        byte[] tmp = new byte[1024];
        while (true) {
            while (in.available() > 0) {
                int i = in.read(tmp, 0, 1024);
                if (i < 0) break;
                log.info(new String(tmp, 0, i));
            }
            if (channel.isClosed()) {
                if (in.available() > 0) continue;
                returnCode = channel.getExitStatus();
                log.info("exit-status: " + channel.getExitStatus());
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (Exception ee) {
            }
        }
        os.close();
        in.close();
        channel.disconnect();
        session.disconnect();
        return returnCode;
    }

    public static void main(String[] args) {
        JSchExecutor jSchUtil = new JSchExecutor("root", "SmkT7i^i", "flydiysz.cn");
        try {
            jSchUtil.connect();
//            jSchUtil.uploadFile("C:\\data\\applogs\\bd-job\\jobhandler\\2020-03-07\\employee.py","/data/applogs/bd-job-777/jobhandler/2018-09-15/employee.py");
//            jSchUtil.execCmd("source /etc/profile && /usr/bin/python2.7 /opt/module/datax/bin/datax.py /opt/module/datax/job/8aaa8452722cf8ad01722d055f300000.json");
            jSchUtil.shellCmd("source /etc/profile && /usr/bin/python2.7 /opt/module/datax/bin/datax.py /opt/module/datax/job/8aaa8452722cf8ad01722d055f300000.json");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jSchUtil.disconnect();
        }

    }

}
