package com.utils;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class JsonUtil {
    private static final Logger logger = LoggerFactory.getLogger(JsonUtil.class);
    private static ObjectMapper mapper = null;
    private static ObjectMapper xmlMapper = null;

    public JsonUtil() {
    }

    public static void setObjectMapper(ObjectMapper objectMapper) {
        handleExtObjectMap(objectMapper);
        mapper = objectMapper;
    }

    private static ObjectMapper getObjectMapper() {
        if (mapper == null) {
            logger.warn("current ObjectMapper is null, use default ObjectMapper!!");
            mapper = new ObjectMapper();
            initMapper(mapper);
        }

        return mapper;
    }

    private static void initMapper(ObjectMapper mapper) {
        DateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        mapper.setDateFormat(fmt);
        mapper.setSerializationInclusion(Include.NON_NULL);
        mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        mapper.configure(DeserializationFeature.USE_LONG_FOR_INTS, true);
        handleExtObjectMap(mapper);
    }

    private static void handleExtObjectMap(ObjectMapper mapper) {
        mapper.getSerializerProvider().setNullKeySerializer(new JsonSerializer() {
            public void serialize(Object key, JsonGenerator jgen, SerializerProvider provider) throws IOException {
                jgen.writeFieldName("");
            }
        });
    }

    public static void registerModule(SimpleModule module) {
        getObjectMapper().registerModule(module);
    }

    public static String toJson(Object obj) {
        if (obj == null) {
            return "";
        } else {
            try {
                StringWriter writer = new StringWriter();
                getObjectMapper().writeValue(writer, obj);
                return writer.toString();
            } catch (Exception var2) {
                logger.error(var2.getMessage(), var2);
                logger.error("toJson error: " + obj);
                throw new RuntimeException("sys.jsonUtil.error", var2);
                
            }
        }
    }

    public static String toJson(Object[] obs) {
        if (obs == null) {
            return "";
        } else {
            try {
                String string = getObjectMapper().writeValueAsString(obs);
                return string;
            } catch (Exception var2) {
                logger.error(var2.getMessage(), var2);
                logger.error("toJson2 error: " + Arrays.asList(obs));
                throw new RuntimeException("sys.jsonUtil.error",  var2);
            }
        }
    }

    public static List<Map> getListByJson(String json) {
        if (StringUtils.isBlank(json)) {
            return new ArrayList();
        } else {
            try {
                List<Map> list = (List)getObjectMapper().readValue(json, List.class);
                return list;
            } catch (Exception var2) {
                logger.error(var2.getMessage(), var2);
                logger.error("getListByJson error: " + json);
                throw new RuntimeException("sys.jsonUtil.error",  var2);
            }
        }
    }

    public static <T> List<T> getListByJson(String json, Class<T> c) {
        if (StringUtils.isBlank(json)) {
            return new ArrayList();
        } else {
            try {
                List<T> list = (List)getObjectMapper().readValue(json, getCollectionType(ArrayList.class, c));
                return list;
            } catch (Exception var3) {
                logger.error(var3.getMessage(), var3);
                logger.error("getListByJson2 error: " + json);
                throw new RuntimeException("sys.jsonUtil.error",  var3);
            }
        }
    }

    private static JavaType getCollectionType(Class<?> collectionClass, Class<?>... elementClasses) {
        return getObjectMapper().getTypeFactory().constructParametricType(collectionClass, elementClasses);
    }

    public static <T> T getObjectByJson(String json, Class<T> c) {
        if (StringUtils.isBlank(json)) {
            return null;
        } else {
            try {
                T t = getObjectMapper().readValue(json, c);
                return t;
            } catch (Exception var3) {
                logger.error(var3.getMessage(), var3);
                logger.error("getObjectByJson error: " + json);
                throw new RuntimeException("sys.jsonUtil.error",  var3);
            }
        }
    }

    public static Map getMapByJson(String json) {
        if (StringUtils.isBlank(json)) {
            return new HashMap();
        } else {
            try {
                HashMap m = (HashMap)getObjectMapper().readValue(json, HashMap.class);
                return m;
            } catch (Exception var2) {
                logger.error(var2.getMessage(), var2);
                logger.error("getMapByJson error: " + json);
                throw new RuntimeException("sys.jsonUtil.error",  var2);
            }
        }
    }

    public static void main(String[] args) {
        List<Map> listByJson = getListByJson("[{\"statusCode\":500,\"message\":\"没有检测到人脸，请更改上传的图片！\",\"timestamp\":\"2020-01-10T08:06:20.945Z\"}]");
        System.out.println();
    }
}

