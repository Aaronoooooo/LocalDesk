package com.javalistmap;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author pengfeisu
 * @create 2020-02-25 19:16
 */
public class ListTest {
    private static final Logger log = LoggerFactory.getLogger(ListTest.class);

    public static void main(String[] args) {
        // List<Student> students = new ListTest().studentList();
        // System.out.println("iterator:" + students);
    }

    public void studentList() {
        // List<Student> students = new ArrayList<>();

        List<String> list = new ArrayList<String>();
        list.add("A");
        list.add("A");
        list.add("C");
        list.add("D");
        System.out.println(list.toString());
        for (int i = 0; i < list.size(); i++) {
            String str = list.get(i);
            if (str.equals("A")) {
                list.remove(i);
            }
        }
        // for (String li : list) {
        //     Student student = new Student();
        //     student.setName(li);
        //     students.add(student);
        // }
    }

    /**
     * 添加：put(Object key,Object value)
     * 删除：remove(Object key)
     * 修改：put(Object key,Object value)
     * 查询：get(Object key)
     * 长度：size()
     * 遍历：keySet() / values() / entrySet()
     */

    @Test
    public void test() {
        Map<String, Object> map = new HashMap<>();
        map.put("a", null);
        map.put("b", "2");
        map.put("c", "3");
        for (Map.Entry<String, Object> s : map.entrySet()) {    //键值对:a=1
            System.out.println("键值对:" + s);
        }
        for (Object s : map.values()) {
            System.out.println("values:" + s);
        }
        for (String s : map.keySet()) {     //key:a,values:1
            System.out.println("key:" + s + ",values:" + map.get(s));
        }
        log.info("打印log日志...........");
        System.out.println(map.getOrDefault("g", "28342849284923"));
    }

    @Test
    public void test1() {
        /*List＜Map＜String, Object＞＞ 里面存放的是map的地址,尽管循环了3次,但是每次的map对应的都是同一个地址,
        所以最后listMap里面存放的是五个同样的map。*/
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        Map<String, Object> map = new HashMap<String, Object>();
        for (int i = 0; i < 3; i++) {
            map.put("a", i);
            map.put("b", i);
            list.add(map);
        }
        System.out.println(list);
        /*[{b=2, a=2}, {b=2, a=2}, {b=2, a=2}]*/
    }

    @Test
    public void test2() {
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
//		Map<String, Object> map = new HashMap<String, Object>();
        for (int i = 0; i < 3; i++) {
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("creTopic", i);
            map.put("numPartition", i);
            map.put("replicationFactor", i);
            list.add(map);
            Object createTopics = map.get("createTopics");
            System.out.print("get(\"createTopics\"): " + createTopics + '\t');
            //get("createTopics"): null	get("createTopics"): null
        }
        System.out.println();
        System.out.println(list);
        //[{replicationFactor=0, createTopics=0, numPartitions=0}, {replicationFactor=1, createTopics=1, numPartitions=1}]
    }

    @Test
    public void test3() {
        Map map = new HashMap<String, Object>();
        map.put("1", "fds");
        map.put("2", "valu");
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        list.add(map);

        for (Map<String, Object> m : list) {
            for (String k : m.keySet()) {
                System.out.print(k + ":" + m.get(k) + '\t');
                /*1 : fds	2 : valu*/
            }
        }
    }

    @Test
    public void test4() {
        Collection coll = new ArrayList();
        coll.add(123);
        coll.add(456);
        coll.add(new String("Tom"));
        coll.add(false);

        //删除集合中"Tom"
        Iterator iterator = coll.iterator();
        while (iterator.hasNext()) {
//            iterator.remove();
            Object obj = iterator.next();
            if ("Tom".equals(obj)) {
                iterator.remove();
//                iterator.remove();
            }

        }
        //遍历集合
        iterator = coll.iterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
    }

    @Test
    public void testList1() {

        //开始(创建List)
        List<String> list = new ArrayList<String>();
        list.add("张学友");
        list.add("刘德华");
        list.add("郭富城");
        list.add("黎明");

        // 判断
        if (!list.isEmpty()) {

            //循环
            for (int i = 0; i < list.size(); i++) {
                String str = list.get(i);

                // 判断
                if (str.equals("黎明")) {
                    list.remove(i);
                }
            }
        }
        //结束
        // return list;
        System.out.println(list.toString());
    }

    @Test
    public void testList2() {

        //开始(创建List)
        List<String> list = new ArrayList<String>();
        list.add("乔峰");
        list.add("姑苏慕容");
        list.add("段誉");
        list.add("金毛狮王");
        list.add("张三丰");

        //循环
        Iterator<String> iterator = list.iterator();
        while (iterator.hasNext()) {
            Object obj = iterator.next();

            // 判断
            if ("姑苏慕容".equals(obj)) {
                iterator.remove();
            }
        }
        //结束
        // return list;
        System.out.println(list.toString());
    }

    @Test
    public void testList3() {

        //开始(创建List)
        List<String> list = new ArrayList<String>();
        list.add("静蕾");
        list.add("周迅");
        list.add("赵薇");
        list.add("章子怡");
        list.add("张三丰");

        //循环
        for (int i = 0; i < list.size(); i++) {
            String str = list.get(i);

            // 判断
            if (str.equals("静蕾")) {
                list.remove(i);
            } else if (str.equals("张三丰")) {
                list.remove(i);
            }
        }
        //结束
        // return list;
        System.out.println(list.toString());
    }

    @Test
    public void testList4() {

        //开始(创建List)
        List<String> list = new ArrayList<String>();
        list.add("杨幂");
        list.add("刘诗诗");
        list.add("倪妮");
        list.add("Angelababy");
        list.add("迪丽热巴");

        //循环
        for (int i = 0; i < list.size(); i++) {
            String str = list.get(i);

            // 判断
            if (str.equals("Angelababy")) {
                list.set(i, "杨颖");
            }
        }
        //结束
        // return list;
        System.out.println(list.toString());
    }

    @Test
    public void testList5() {

        //开始(创建List)
        List<String> list = new ArrayList<String>();
        // list.add("四小花旦");
        list.add("梅兰芳");
        list.add("程砚秋");
        list.add("尚小云");
        list.add("荀慧生");
        list.add("关晓彤");

        //循环
        for (int i = 0; i < list.size(); i++) {

            // 判断
            String str = list.get(i);
            if ("关晓彤".equals(str)) {
                list.add("鹿晗");
            } else {
                System.out.println(str);
            }

        }
        //结束
        // return list;
        System.out.println(list.toString());
    }
}
