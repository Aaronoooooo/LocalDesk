package com.vince.xq.common;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.InputStream;

public class Configs {

    public static ParameterTool loadConfig(String configFileName) throws Exception {
        try (InputStream is = Configs.class.getClassLoader().getResourceAsStream(configFileName)) {
            return ParameterTool.fromPropertiesFile(is);
        }
    }

}
