public class TeacherTest {
    public static void main(String[] args) {
        int age=10;
        String sex = "man";

        StudentTest studentTest = new StudentTest(age, sex, "a", "b", "c");
        System.out.println(studentTest);
    }
}
