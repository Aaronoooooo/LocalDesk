import java.util.*;

public List autoCodeTest(String in){

//开始(创建List)
        List<String> list=new ArrayList<String>();
        list.add("张三")
        list.add("李四")
        list.add("王五")
        list.add("赵六")

//判断1
        if(!list.isEmpty()()){

//循环
        for(int i=0;i<list.size();i++){
        String str=list.get(i);
//判断2
        if(str.equals("孙九")){

//修改
        list.remove(i);
        }
        }
        }
//结束
        return list;
        System.out.println(list.toString());
        }