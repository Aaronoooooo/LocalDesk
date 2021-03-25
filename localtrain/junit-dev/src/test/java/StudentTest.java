import java.util.Arrays;

public class StudentTest {
    public int age;
    public String sex;
    public String[] name;

    public StudentTest() {
    }

    public StudentTest(int age, String sex, String... name) {
        this.age = age;
        this.sex = sex;
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String[] getName() {
        return name;
    }

    public void setName(String[] name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "StudentTest{" +
                "age=" + age +
                ", sex='" + sex + '\'' +
                ", name=" + Arrays.toString(name) +
                '}';
    }
}
