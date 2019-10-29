import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

/**
 * @author smi1e
 * Date 2019/10/28 16:03
 * Description
 */
public class ServerTest {
    public static void main(String[] args) {
        try {
            ServerSocket serverSocket = new ServerSocket(9999);
            System.out.println("启动成功");
            Socket socket = serverSocket.accept();
            System.out.println("连接成功，请输入要发送的信息：");
            OutputStream os = socket.getOutputStream();
            PrintWriter printWriter = new PrintWriter(os);
            Scanner keyboardInput = new Scanner(System.in);
            while (true) {
                String line = keyboardInput.nextLine();
                if ("END".equals(line)){
                    break;
                }
                printWriter.println(line);
                printWriter.flush();
            }
            keyboardInput.close();
            printWriter.close();
            socket.close();
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
