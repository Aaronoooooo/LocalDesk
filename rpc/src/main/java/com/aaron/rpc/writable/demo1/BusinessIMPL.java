package com.aaron.rpc.writable.demo1;

import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;

public class BusinessIMPL implements BussinessProtocol  {

    @Override
    public void mkdir(String path) {
        System.out.println("成功创建文件夹 : " + path);
    }

    @Override
    public String getName(String name) {
        System.out.println("成功打招呼: hello : " + name);
        return "bigdata";
    }
}
