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
        try(s){
            System.out.println("response: " + s);
        }
        catch(Exception e){
            System.out.println("error in handling " + e);
        }
    }
}
