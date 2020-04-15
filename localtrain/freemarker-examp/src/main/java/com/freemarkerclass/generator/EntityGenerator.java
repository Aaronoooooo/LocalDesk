package com.freemarkerclass.generator;

/**
 * @author pengfeisu
 * @create 2020-02-12 13:47
 */

import com.freemarkerclass.vo.*;
import freemarker.template.TemplateException;

import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * 实体类生成器
 */
public class EntityGenerator extends AbstractGenerator {

    public EntityGenerator(String ftl) throws Exception {
        try {
            this.savePath = "D:\\object\\itripljh\\itripbeans\\src\\main\\java\\cn\\itrip\\pojo";
            //加载freemarker模板文件
            super.template = cfg.getTemplate(ftl);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public void generateCode() throws Exception {
        System.out.println("---------------开始生成代码");
        valueMap.put("package", "cn.itrip");//自定义包的位置
        OutputStreamWriter writer = null;
        for (TableVo table : tableList) {
            valueMap.put("table", table);
            try {
                //创建一个写入器
                writer = new FileWriter(savePath + "/" + table.getClassName() + ".java");//根据不同的文件自行修改
                //合成数据和模板，写入到文件
                this.template.process(valueMap, writer);
                writer.flush();
            } catch (TemplateException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                writer.close();
            }
        }
        System.out.println("生成代码成功！");
    }
}