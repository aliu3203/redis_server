import java.net.*;
import java.io.*;

public class Server{

    private static final int PORT = 6380;

    public static void main(String[] args){
        try(ServerSocket ss = new ServerSocket(PORT)){
            while(true){
                try{
                    Socket s = ss.accept();
                    Thread.ofVirtual().start(() -> handle(s));
                }
                catch(Exception e){
                    System.out.println("accept failed: " + e);
                }
            }
        }
        catch (Exception e){
            System.out.println("error " + e);
        }
    }

    private static void handle(Socket s){
        try(s;
            InputStream in = s.getInputStream();
            OutputStream out = s.getOutputStream();
        ){
            byte[] buf = new byte[256];
            int n;
            while((n = in.read(buf)) != -1){
                if(n > 0){
                    System.out.println("request " + new String(buf, 0, n));
                }
            }

            
        }
        catch(Exception e){
            System.out.println("error in handling " + e);
        }
    }
}
