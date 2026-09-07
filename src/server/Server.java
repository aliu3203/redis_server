package server;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

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
            
            Buffer buffer = new Buffer();

            while(true){
                // read
                int n = buffer.read(in);
                if(n == -1){
                    break;
                }
                

                Command cmd;
                while((cmd = Parser.tryParse(buffer)) != null){
                    dispatch(cmd);
                }
            }
            
        }
        catch(Exception e){
            System.out.println("error in handling " + e);
        }
    }

    private static void dispatch(Command cmd){

    }

}
