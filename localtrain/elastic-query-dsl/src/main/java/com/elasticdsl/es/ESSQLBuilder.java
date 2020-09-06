package com.elasticdsl.es;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.*;

/**
 * Created by player on 2020/3/19.
 */
public class ESSQLBuilder {

    private static final Map<String, List<Field>> cacheFields = new HashMap();

    private static final Logger logger = LoggerFactory.getLogger(ESSQLBuilder.class);

    public static String getESSQL(SearchParam searchParam){
        BaseCriteria criteria = (BaseCriteria)searchParam.getCriteria();
        //String index = getIndexName(searchParam);
        String index = searchParam.getIndex();
        StringBuffer stringBuffer = new StringBuffer();
        //TODO * 号可以做优化处理
        stringBuffer.append("select * from "+index+ " where 1=1 ");

        stringBuffer.append(handleSearchParam(criteria));

        //默认就按日志时间排序
        stringBuffer.append(" order by logDateTime desc");
        return stringBuffer.toString();
    }

    private static List<Field> getFieldList(Class tempClass) {
        String curBeanName = tempClass.getName();
        List<Field> curBeanFields = (List)cacheFields.get(curBeanName);
        if (curBeanFields != null) {
            return curBeanFields;
        } else {
            ArrayList fieldList;
            for(fieldList = new ArrayList(); tempClass != null; tempClass = tempClass.getSuperclass()) {
                List list = Arrays.asList(tempClass.getDeclaredFields());
                if (list.size() > 0) {
                    fieldList.addAll(list);
                }
            }

            cacheFields.put(curBeanName, fieldList);
            return fieldList;
        }
    }

    private static boolean isOrSpecification(String methodName) {
        return "get_OR_".equals(methodName);
    }

    private static String handleSearchParam(BaseCriteria baseCriteria){
        StringBuffer stringBuffer = new StringBuffer();
        if (null != baseCriteria) {
            List list = getFieldList(baseCriteria.getClass());

            try {
                for(int i = 0; i < list.size(); ++i) {
                    Field field = (Field)list.get(i);
                    Class type = field.getType();
                    String name = field.getName();
                    String methodName = "get" + name.substring(0, 1).toUpperCase() + name.substring(1);
                    if (Filter.class.isAssignableFrom(type) || isOrSpecification(methodName)) {
                        Method method = baseCriteria.getClass().getMethod(methodName);
                        Object val = method.invoke(baseCriteria);
                        logger.debug("loop.i=" + i + ",methodName=" + methodName + ",val=" + val);
                        if (null != val) {
                            if (isOrSpecification(methodName)) {
                                //todo 暂不处理or情况
                                throw new RuntimeException("暂时还没处理OR条件");
                            } else if (RangeFilter.class.isAssignableFrom(type)) {
                                stringBuffer.append(buildRangeSpecification((RangeFilter)val, name));
                            } else if (StringFilter.class.isAssignableFrom(type)) {
                                stringBuffer.append(buildStringSpecification((StringFilter)val, name));
                            } else {
                                throw new RuntimeException("找不到相应的Filter解析器");
                            }
                        }
                    }
                }
            } catch (Exception var12) {
                new Throwable(var12);
            }
        }
        return stringBuffer.toString();
    }

    private static String buildRangeSpecification(RangeFilter filter, String name) {
        String sql = "";
        if (filter.getGreaterThan() != null) {
            sql += " and ";
            Instant greaterThan = (Instant) filter.getGreaterThan();
            sql += name + " > '"+ greaterThan +"'";
//            Date date = new Date(greaterThan.toEpochMilli());
//            sql += name + " > '"+ DateUtil.toDateString(date,"yyyy-MM-dd HH:mm:ss.SSS") +"'";
        }

        if (filter.getGreaterOrEqualThan() != null) {
            sql += " and ";
            Instant greaterOrEqualThan = (Instant) filter.getGreaterOrEqualThan();
            sql += name + " >= '"+ greaterOrEqualThan +"'";
//            Date date = new Date(greaterOrEqualThan.toEpochMilli());
//            sql += name + " >= '"+DateUtil.toDateString(date,"yyyy-MM-dd HH:mm:ss.SSS")+"'";
        }

        if (filter.getLessThan() != null) {
            sql += " and ";
            Instant lessThan = (Instant) filter.getLessThan();
            sql += name + " < '"+ lessThan +"'";
//            Date date = new Date(lessThan.toEpochMilli());
//            sql += name + " < '"+DateUtil.toDateString(date,"yyyy-MM-dd HH:mm:ss.SSS")+"'";
        }

        if (filter.getLessOrEqualThan() != null) {
            sql += " and ";
            Instant lessOrEqualThan = (Instant) filter.getLessOrEqualThan();
            sql += name + " <= '"+ lessOrEqualThan +"'";
//            Date date = new Date(lessOrEqualThan.toEpochMilli());
//            sql += name + " <= '"+ DateUtil.toDateString(date,"yyyy-MM-dd HH:mm:ss.SSS") +"'";
        }

        return  sql;
    }

    private static String buildStringSpecification(StringFilter filter, String name) {
        StringBuffer sql = new StringBuffer();

        if (StringUtils.isNotEmpty(filter.getContains())) {
//            if(isEnglish(filter.getContains())){//如果是纯英文就用like
            sql.append(" and " + name+" like '%"+filter.getContains()+"%'");
//            }else {
//                sql.append(" and match("+name+",'"+filter.getContains()+"') ") ;
//            }
        }
        List<String> in = filter.getIn();
        if(CollectionUtils.isNotEmpty(in)){
            sql.append(" and " + name+" in (");
            for (String str : in) {
                sql.append("'" + str + "',");
            }
            sql.deleteCharAt(sql.length() - 1);
            sql.append(") ");
        }
        if(StringUtils.isNotEmpty(filter.getStartsWith())) { //生成  value%  查询条件
            sql.append(" and " + name+" like '"+filter.getStartsWith()+"'");
        }
        if(StringUtils.isNotEmpty(filter.getEndsWith()) ) { //生成  %value  查询条件
            sql.append(" and " + name+" like '"+filter.getEndsWith()+"'");
        }
        if(StringUtils.isNotEmpty((String) filter.getEquals())){
            sql.append(" and " + name+"  = '"+filter.getEquals()+"'");
        }

        return sql.toString();
    }
}
