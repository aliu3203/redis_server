package server;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.function.BooleanSupplier;

// Routes a parsed Command to its implementation and writes exactly one reply.
// This is the only layer that knows what commands mean -- Buffer knows bytes,
// Parser knows RESP syntax, neither knows that GET reads and SET writes.
public final class Dispatcher{

    private final Keyspace keyspace;

    public Dispatcher(Keyspace keyspace){
        this.keyspace = keyspace;
    }

    public void dispatch(Command cmd, OutputStream out, BooleanSupplier clientGone) throws IOException{
        // Empty inline line or *0 -- Redis sends no reply at all. The one
        // legitimate exception to one-command-one-reply.
        if(cmd.argc() == 0){
            return;
        }

        try{
            switch(cmd.name()){
                case "PING" -> ping(cmd, out);
                case "ECHO" -> echo(cmd, out);
                case "GET"  -> get(cmd, out);
                case "SET"  -> set(cmd, out);
                case "INCR" -> incr(cmd, out);
                case "LPUSH" -> lpush(cmd, out);
                case "RPUSH" -> rpush(cmd, out);
                case "LPOP"  -> lpop(cmd, out);
                case "RPOP"  -> rpop(cmd, out);
                case "LLEN"  -> llen(cmd, out);
                case "BLPOP" -> blpop(cmd, out, clientGone);
                default     -> Reply.error(out, "ERR unknown command '" + cmd.name() + "'");
            }
        }
        catch(RuntimeException e){
            // A bug in one command must not drop the connection. Framing is
            // still intact here, so reply and keep going.
            Reply.error(out, "ERR internal error");
        }
    }

    private void ping(Command cmd, OutputStream out) throws IOException{
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

    private void echo(Command cmd, OutputStream out) throws IOException{
        if(cmd.argc() == 2){
            Reply.bulk(out, cmd.arg(1));
        }
        else{
            Reply.error(out, wrongArgs("echo"));
        }
    }

    private void get(Command cmd, OutputStream out) throws IOException{
        if(cmd.argc() != 2){
            Reply.error(out, wrongArgs("get"));
            return;
        }
        String key = new String(cmd.arg(1), StandardCharsets.ISO_8859_1);

        RedisValue value = keyspace.get(key);

        if(value == null){
            Reply.nullBulk(out);
        }
        else if(value instanceof RedisValue.Str s){
            Reply.bulk(out, s.bytes());
        }
        else{
            Reply.error(out, WrongTypeException.MESSAGE);
        }
    }

    private void set(Command cmd, OutputStream out) throws IOException{
        if(cmd.argc() != 3){
            Reply.error(out, wrongArgs("set"));
            return;
        }
        String key = new String(cmd.arg(1), StandardCharsets.ISO_8859_1);

        keyspace.set(key, cmd.arg(2));
        Reply.simple(out, "OK");
    }

    private void incr(Command cmd, OutputStream out) throws IOException{
        if(cmd.argc() != 2){
            Reply.error(out, wrongArgs("incr"));
            return;
        }
        String key = new String(cmd.arg(1), StandardCharsets.ISO_8859_1);
        try{
            Reply.integer(out, keyspace.incr(key));
        }
        catch(NumberFormatException e){
            Reply.error(out, "ERR value is not an integer or is out of range");
        }  
        catch(ArithmeticException e){
            Reply.error(out, "ERR incr overflows");
        }
        catch(WrongTypeException e){
            // Thrown from inside Keyspace.incr's compute lambda, where the type
            // check has to live. Message comes from the exception so the wording
            // cannot drift from the one get() writes.
            Reply.error(out, e.getMessage());
        }
    }

    // ---- list commands ---------------------------------------------------

    // LPUSH key value  ->  :<new length>
    private void lpush(Command cmd, OutputStream out) throws IOException{
        if(cmd.argc() != 3){
            Reply.error(out, wrongArgs("lpush"));
            return;
        }
        String key = new String(cmd.arg(1), StandardCharsets.ISO_8859_1);
        try{
            Reply.integer(out, keyspace.lpush(key, cmd.arg(2)));
        }
        catch(WrongTypeException e){
            Reply.error(out, e.getMessage());
        }
    }

    private void rpush(Command cmd, OutputStream out) throws IOException{
        if(cmd.argc() != 3){
            Reply.error(out, wrongArgs("rpush"));
            return;
        }
        String key = new String(cmd.arg(1), StandardCharsets.ISO_8859_1);
        try{
            Reply.integer(out, keyspace.rpush(key, cmd.arg(2)));
        }
        catch(WrongTypeException e){
            Reply.error(out, e.getMessage());
        }
    }

    // LPOP key  ->  $<len>value   or   $-1 if the key is absent
    private void lpop(Command cmd, OutputStream out) throws IOException{
        if(cmd.argc() != 2){
            Reply.error(out, wrongArgs("lpop"));
            return;
        }
        String key = new String(cmd.arg(1), StandardCharsets.ISO_8859_1);
        try{
            byte[] popped = keyspace.lpop(key);
            if(popped == null){
                Reply.nullBulk(out);
            }
            else{
                Reply.bulk(out, popped);
            }
        }
        catch(WrongTypeException e){
            Reply.error(out, e.getMessage());
        }
    }

    private void rpop(Command cmd, OutputStream out) throws IOException{
        if(cmd.argc() != 2){
            Reply.error(out, wrongArgs("rpop"));
            return;
        }
        String key = new String(cmd.arg(1), StandardCharsets.ISO_8859_1);
        try{
            byte[] popped = keyspace.rpop(key);
            if(popped == null){
                Reply.nullBulk(out);
            }
            else{
                Reply.bulk(out, popped);
            }
        }
        catch(WrongTypeException e){
            Reply.error(out, e.getMessage());
        }
    }

    // LLEN key  ->  :<n>   (0 when absent, WRONGTYPE when it is a string)
    private void llen(Command cmd, OutputStream out) throws IOException{
        if(cmd.argc() != 2){
            Reply.error(out, wrongArgs("llen"));
            return;
        }
        String key = new String(cmd.arg(1), StandardCharsets.ISO_8859_1);
        try{
            Reply.integer(out, keyspace.llen(key));
        }
        catch(WrongTypeException e){
            Reply.error(out, e.getMessage());
        }
    }

    private void blpop(Command cmd, OutputStream out, BooleanSupplier clientGone) throws IOException{
        if(cmd.argc() < 3){
            Reply.error(out, wrongArgs("blpop"));
            return;
        }

        // TEMP
        if(cmd.argc() > 3){
            Reply.error(out, "ERR multi-key BLPOP not supported yet");
            return;
        }

        String key = new String(cmd.arg(1), StandardCharsets.ISO_8859_1);

        String t = new String(cmd.arg(cmd.argc() - 1), StandardCharsets.ISO_8859_1);
        double seconds;
        try{
            seconds = Double.parseDouble(t);
        } catch(NumberFormatException e){
            Reply.error(out, "ERR timeout is not a float or out of range");
            return;
        }
        if(seconds < 0 || Double.isNaN(seconds) || Double.isInfinite(seconds)){
            Reply.error(out, "ERR timeout is negative");
            return;
        }
        long timeoutMs = (long)(seconds * 1000);     // 0 = block forever

        if(seconds > 0 && timeoutMs == 0){
            timeoutMs = 1;
        }
        out.flush();
        try{
            Popped p = keyspace.blpop(key, timeoutMs, clientGone);
            if(p == null){
                Reply.nullArray(out);
            } else {
                Reply.arrayHeader(out, 2);
                Reply.bulk(out, p.key().getBytes(StandardCharsets.ISO_8859_1));
                Reply.bulk(out, p.value());
            }
        }
        catch(WrongTypeException e){
            Reply.error(out, e.getMessage());
        }
        catch(InterruptedException e){
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while blocked in BLPOP", e);
        }
        catch(ClientGoneException e){
            // Keyspace has already handed back anything the client was owed;
            // close the connection without writing to it.
            throw new IOException(e.getMessage(), e);
        }
    }

    private String wrongArgs(String name){
        return "ERR wrong number of arguments for '" + name + "' command";
    }
}
