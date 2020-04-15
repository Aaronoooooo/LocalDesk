package ${classPath};

public class ${className} {

public static void main(String[] args) {

System.out.println("${helloWorld}");

 }

}

public class ${0} {

private ${1} ${2};
private ${3} ${4};

public ${0}() {
}

public ${0}(${1} ${2}, ${3} ${4}) {
this.${2} = ${2};
this.${4} = ${4};
}

public Integer getId() {
return id;
}

public void setId(Integer id) {
this.id = id;
}

public String getName() {
return name;
}

public void setName(String name) {
this.name = name;
}

@Override
public String toString() {
return "User [id=" + id + ", name=" + name + "]";
}

}