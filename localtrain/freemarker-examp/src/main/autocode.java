for (int i = 0; i < userList.size(); i++) {
Integer in = userList.get(i);
if (in.equals(10)) {
    user.setName("张三三");
}else {
System.out.println("年龄不匹配");
}
}