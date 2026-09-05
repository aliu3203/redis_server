import java.util.*;
import java.io.*;
import java.net.*;

public class test{
    public static void main(String[] args) throws Exception{
        byte[] ping = "*1\r\n$4\r\nPING\r\n".getBytes();
        
        try(Socket s = new Socket("127.0.0.1", 6380)){
            OutputStream out = s.getOutputStream();
            for(byte b : ping){
                out.write(b);
                out.flush();
                Thread.sleep(5);
            }

        }
    }
}
