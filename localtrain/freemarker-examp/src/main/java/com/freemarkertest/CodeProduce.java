package com.freemarkertest;


import freemarker.template.Configuration;
import freemarker.template.TemplateExceptionHandler;

/**
 * Created by admin on 2017/1/6.
 */
public class CodeProduce {
    public static final String CHARSET = "UTF-8";

    static final Configuration cfg = new Configuration(Configuration.VERSION_2_3_22);
    // static final String TEMPLATE_PATH = "src/main/resources/templates";
    static {

        //设置默认编码格式
        cfg.setDefaultEncoding(CHARSET);
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
        cfg.setLogTemplateExceptions(false);

        //处理未定义的表达式
        cfg.setClassicCompatible(true);
        // cacheTemplate();

    }

}
