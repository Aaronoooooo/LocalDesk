import java.util.HashMap;


class People{
    private String name;
    private int age;

    public People(String name,int age) {
        this.name = name;
        this.age = age;
    }

    public void setAge(int age){
        this.age = age;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object obj) {
        // TODO Auto-generated method stub
        return this.name.equals(((People)obj).name) && this.age== ((People)obj).age;
    }
}

public class Main {

    public static void main(String[] args) {

        People p1 = new People("Jack", 12);
        String name = p1.getName();
        int i = name.hashCode();
        String s = String.valueOf(i);
        String substring = s.substring(0, 3);
        String nameChange = substring + name;
        System.out.println(nameChange);
        System.out.println(p1.hashCode());

        HashMap<People, Integer> hashMap = new HashMap<People, Integer>();
        hashMap.put(p1, 1);

        System.out.println(hashMap.get(new People("Jack", 12)));
    }
}