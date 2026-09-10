package server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.function.BooleanSupplier;

public class Server{

    private static final int PORT = 6380;

    public static void main(String[] args){

        Keyspace ks = new Keyspace();
        Dispatcher d = new Dispatcher(ks);

        try(ServerSocket ss = new ServerSocket(PORT)){
            while(true){
                try{
                    Socket s = ss.accept();
                    Thread.ofVirtual().start(() -> handle(s, d));
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

    private static void handle(Socket s, Dispatcher d){
        try(s;
            InputStream in = s.getInputStream();
            OutputStream out = new BufferedOutputStream(s.getOutputStream());
        ){
            
            Buffer buffer = new Buffer();

            // Lets a blocking command (BLPOP) ask whether this client is still
            // connected. Only ever called on this thread, which owns s and buffer.
            BooleanSupplier gone = () -> clientGone(s, in, buffer);

            while(true){
                // read
                int n = buffer.read(in);
                if(n == -1){
                    break;
                }
                

                Command cmd;
                while((cmd = Parser.tryParse(buffer)) != null){
                    d.dispatch(cmd, out, gone);
                }
                out.flush();
            }
            
        }
        catch(Exception e){
            System.out.println("error in handling " + e);
        }
    }

    // Checks, without blocking, whether the client has closed the connection.
    //
    // Writing can't tell you: the first write to a closed socket usually
    // succeeds (the OS accepts the bytes; only a later write fails). Reading
    // can: -1 means the client closed. Any bytes that do arrive -- commands
    // pipelined behind a blocking one -- go into this connection's Buffer and
    // are parsed once the blocking command finishes.
    private static boolean clientGone(Socket s, InputStream in, Buffer buffer){
        try{
            s.setSoTimeout(1);
            return buffer.read(in) == -1;
        }
        catch(SocketTimeoutException e){
            return false;                  // nothing to read: still connected
        }
        catch(IOException e){
            return true;                   // connection broken
        }
        finally{
            try{
                s.setSoTimeout(0);
            }
            catch(IOException ignored){
            }
        }
    }

}
