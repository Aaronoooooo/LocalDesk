package com.sshclient;

/**
 * @author pengfeisu
 * @create 2020-04-21 10:53
 */
public class SSHClientOutput {
    private String text;
    private int exitCode = -1;

    /**
     * @param text
     * @param exitCode
     */
    public SSHClientOutput(int exitCode, String text) {
        this.text = text;
        this.exitCode = exitCode;
    }

    /**
     * @return the text
     */
    public String getText() {
        return text.toString();
    }

    /**
     * @return the exitCode
     */
    public int getExitCode() {
        return exitCode;
    }

}
