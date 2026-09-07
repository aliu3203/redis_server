package server;

import java.util.ArrayList;
import java.util.List;


public final class Parser{

    private static final long MAX_ARGS   = 1024 * 1024;
    private static final long MAX_BULK = 512 * 1024 * 1024;
    private static final long MAX_INLINE = 64 * 1024;
    private static final long MAX_ALLOWED_INT = 512 * 1024 * 1024;


    private Buffer buf;

    // pos represents position from readPos
    private int pos = 0;

    private Parser(Buffer buf){
        this.buf = buf;
    }

    public static Command tryParse(Buffer buf) throws ProtocolError{

        Parser p = new Parser(buf);
        Command cmd = p.parseCommand();
        if(cmd != null){ buf.consume(p.pos); }
        return cmd;

    }

    // Should update pos to byte immediately following command
    private Command parseCommand() throws ProtocolError{
        if(buf.readableBytes(pos) == 0){
            return null;
        }
        
        return ((buf.at(pos) == '*') ? parseMultibulk() : parseInline());
    }
    
    private Command parseMultibulk() throws ProtocolError{
        // get length of args
        int posCRLF = buf.findCRLF(pos+1);
        
        // Incomplete command
        if(posCRLF == -1){
            return null;
        }

        int count = parseInteger(pos+1, posCRLF);

        if(count > MAX_ARGS){
            throw new ProtocolError("too many arguments");
        }

        byte[][] args = new byte[count][];
        pos = posCRLF + 2;

        for(int i = 0; i < count; i++){
            byte[] arg = parseString();

            // unfinished argument
            if(arg == null){
                return null;
            }

            args[i] = arg;
   
        }
        return new Command(args);

    }

    private Command parseInline() throws ProtocolError{
        
        int posCRLF = buf.findCRLF(pos);

        int lineLen = (posCRLF == -1) ? buf.readableBytes(pos) : posCRLF;

        if (lineLen > MAX_INLINE){
            throw new ProtocolError("too big inline request");
        }
        if (posCRLF == -1){
            return null;
        }

        List<byte[]> args = new ArrayList<>();
        int tokenStart = pos;
        for(int i = pos; i <= posCRLF; i++){
            if(i == posCRLF || buf.at(i) == ' '){
                

                // Ensure groups of spaces aren't counted
                if(i > tokenStart){
                    byte[] arg = new byte[i-tokenStart];
                    for(int k = 0; k < i-tokenStart; k++){
                        arg[k] = buf.at(k+tokenStart);
                    }
                    args.add(arg);
                }
                tokenStart = i + 1;
            }
        }
        pos = posCRLF + 2;
        return new Command(args.toArray(new byte[0][]));

    }

    // parses interval [from, to)
    private int parseInteger(int from, int to) throws ProtocolError{
        if(from >= to){
            throw new ProtocolError("empty length");
        }
        long pInt = 0;
        for(int i = from; i < to; i++){
            byte b = buf.at(i);
            if(b >= '0' && b <= '9'){
                pInt = pInt * 10 + (b-'0');
            }
            else{
                throw new ProtocolError("invalid character in length");
            }
            if(pInt > MAX_ALLOWED_INT){
                throw new ProtocolError("exceeded max allowed length");
            }
        }
        return (int)pInt;
    }

    private byte[] parseString() throws ProtocolError{
        if(buf.readableBytes(pos) == 0){
            return null;
        }
        if(buf.at(pos) != '$'){
            throw new ProtocolError("length of string does not begin with dollar sign");
        }
        pos += 1;
        int posCRLF = buf.findCRLF(pos);
        if(posCRLF == -1){
            return null;
        }

        int len = parseInteger(pos, posCRLF);

        if(len > MAX_BULK){
            throw new ProtocolError("number of bytes in bulk command exceeds limit");
        }

        pos = posCRLF + 2;
        if(buf.readableBytes(pos) < len + 2){
            return null;
        }

        if(buf.at(pos + len) != '\r' || buf.at(pos + len + 1) != '\n'){
            throw new ProtocolError("args lied about byte length");
        }

        byte[] arg = new byte[len];
        for(int i = 0; i < len; i++){
            byte b = buf.at(i + posCRLF + 2);
            arg[i] = b;
        }
        pos += len+2;
        return arg;

    }

}
