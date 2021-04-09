package cn.flydiy.module;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class ReflectUtils {

    /**
     * 反射获取实体类中所有的字段
     * @param o
     * @return
     */
    public static List<String> getModelFields(Object o) {
        Class cls = o.getClass();
        Field[] fields = cls.getDeclaredFields();
        List<String> list = new ArrayList<>();
        for (Field f : fields) {
            f.setAccessible(true);
            list.add(f.getName());
        }
        return list;
    }
}
