import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

/**
 * @author smi1e
 * Date 2019/10/28 17:02
 * Description
 */
public class ClientTest {
    public static void main(String[] args) {
        try {
            Socket socket = new Socket("localhost", 9999);
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String line = null;
            while (true) {
                line = reader.readLine();
                System.out.println(line);
            }
        } catch (IOException e) {
        }

    }
}
