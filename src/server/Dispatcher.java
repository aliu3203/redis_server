package server;

import java.io.IOException;
import java.io.OutputStream;

// Routes a parsed Command to its implementation and writes exactly one reply.
// This is the only layer that knows what commands mean -- Buffer knows bytes,
// Parser knows RESP syntax, neither knows that GET reads and SET writes.
public final class Dispatcher{

    private Dispatcher(){}

    public static void dispatch(Command cmd, OutputStream out) throws IOException{
        // Empty inline line or *0 -- Redis sends no reply at all. The one
        // legitimate exception to one-command-one-reply.
        if(cmd.argc() == 0){
            return;
        }

        try{
            switch(cmd.name()){
                case "PING" -> ping(cmd, out);
                case "ECHO" -> echo(cmd, out);
                default     -> Reply.error(out, "ERR unknown command '" + cmd.name() + "'");
            }
        }
        catch(RuntimeException e){
            // A bug in one command must not drop the connection. Framing is
            // still intact here, so reply and keep going.
            Reply.error(out, "ERR internal error");
        }
    }

    private static void ping(Command cmd, OutputStream out) throws IOException{
        if(cmd.argc() == 1){
            Reply.simple(out, "PONG");
        }
        else if(cmd.argc() == 2){
            Reply.bulk(out, cmd.arg(1));
        }
        else{
            Reply.error(out, wrongArgs("ping"));
        }
    }

    private static void echo(Command cmd, OutputStream out) throws IOException{
        if(cmd.argc() == 2){
            Reply.bulk(out, cmd.arg(1));
        }
        else{
            Reply.error(out, wrongArgs("echo"));
        }
    }

    private static String wrongArgs(String name){
        return "ERR wrong number of arguments for '" + name + "' command";
    }
}
