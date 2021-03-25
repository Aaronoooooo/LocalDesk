import java.net.ServerSocket;
import java.net.Socket;

/**
 * @author zxjgreat
 * @create 2020-05-19 8:56
 */
public class Test_net_server {
    public static void main(String[] args) throws Exception {

        //服务器
        ServerSocket server = new ServerSocket(9999);
        //接收
        Socket accept = server.accept();

        int data = accept.getInputStream().read();
        System.out.println("服务器读取的数据为" + data);


    }
}
