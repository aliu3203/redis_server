package server;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

// Encodes RESP replies. The mirror of Parser: Parser turns bytes into a
// Command, Reply turns a result into bytes.
//
// Never flushes -- handle() flushes once after draining a batch of commands,
// so pipelined replies go out in a single syscall.
public final class Reply{

    private static final byte[] CRLF = {'\r', '\n'};

    // Simple strings and errors are line-delimited, so any client-supplied text
    // put inside one must not contain CRLF or it would inject a second reply.
    private static final int MAX_MSG = 128;

    private Reply(){}

    // +OK\r\n
    public static void simple(OutputStream out, String s) throws IOException{
        out.write('+');
        out.write(sanitize(s));
        out.write(CRLF);
    }

    // -ERR ...\r\n
    public static void error(OutputStream out, String msg) throws IOException{
        out.write('-');
        out.write(sanitize(msg));
        out.write(CRLF);
    }

    // :42\r\n
    public static void integer(OutputStream out, long n) throws IOException{
        out.write(':');
        out.write(ascii(Long.toString(n)));
        out.write(CRLF);
    }

    // $5\r\nhello\r\n  -- length prefixed, so the payload may contain anything
    public static void bulk(OutputStream out, byte[] data) throws IOException{
        out.write('$');
        out.write(ascii(Integer.toString(data.length)));
        out.write(CRLF);
        out.write(data);
        out.write(CRLF);
    }

    // $-1\r\n  -- "no such key", distinct from an empty string $0\r\n\r\n
    public static void nullBulk(OutputStream out) throws IOException{
        out.write('$');
        out.write(ascii("-1"));
        out.write(CRLF);
    }

    // *3\r\n  -- header only; caller writes the elements
    public static void arrayHeader(OutputStream out, int count) throws IOException{
        out.write('*');
        out.write(ascii(Integer.toString(count)));
        out.write(CRLF);
    }

    private static byte[] ascii(String s){
        return s.getBytes(StandardCharsets.ISO_8859_1);
    }

    // Truncate and strip CRLF so client-supplied text cannot terminate the
    // line early and inject an extra reply.
    private static byte[] sanitize(String s){
        if(s.length() > MAX_MSG){
            s = s.substring(0, MAX_MSG);
        }
        byte[] b = ascii(s);
        for(int i = 0; i < b.length; i++){
            if(b[i] == '\r' || b[i] == '\n'){
                b[i] = ' ';
            }
        }
        return b;
    }
}
