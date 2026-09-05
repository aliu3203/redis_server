package server;

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
            int writePos = 0; // start writing at writePos

            while(true){
                
                int n = in.read(buf, writePos, buf.length-writePos);
                if(n == -1){
                    break;
                }
                String str = new String(buf);
                System.out.println(str);
                writePos += n;

                Command cmd;
                while((cmd = tryParse(buf)) != null){
                    dispatch()
                }

            }
            
        }
        catch(Exception e){
            System.out.println("error in handling " + e);
        }
    }
}
