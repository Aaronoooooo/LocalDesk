import java.net.ServerSocket;
import java.net.Socket;

/**
 * * @create 2020-05-19 8:56
 */
public class Test_net_client {
    public static void main(String[] args) throws Exception {

        //客户端
        Socket socket = new Socket("localhost",9999);

        //output
        socket.getOutputStream().write(10);
        System.out.println("客户端发送的数据为 10");
        socket.close();

    }
}
