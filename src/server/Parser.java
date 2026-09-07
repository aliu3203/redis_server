package server;

public final class Parser{

    private final long MAX_ALLOWED = 16384;

    private Buffer buf;

    // pos represents position from readPos
    private int pos = 0;

    private Parser(Buffer buf){
        this.buf = buf;
    }

    public static Command tryParse(Buffer buf) throws ProtocolException{

        Parser p = new Parser(buf);
        Command cmd = p.parseCommand();
        if(cmd != null){ buf.consume(p.pos); }
        return cmd;

    }

    // Should update pos to byte immediately following command
    private Command parseCommand() throws ProtocolException{
        if(buf.readableBytes() == 0){
            return null;
        }
        
        return ((buf.at(pos) == '*') ? parseMultibulk : parseInline)
    }
    
    private Command parseMultibulk(){
        // get length of args
        int posCRLF = buf.findCRLF(pos+1);
        
        // Incomplete
        if(posCRLF == -1){
            return null;
        }
        
        int count = parseInteger(pos+1, posCRLF);

        Command cmd = new Command(count);


    }

    private Command parseInline(){
        
    }

    // parses interval [from, to)
    private int parseInteger(int from, int to) throws ProtocolException{
        if(from >= to){
            throw new ProtocolException("empty length");
        }
        long pInt = 0;
        for(int i = from; i < to; i++){
            byte b = buf.at(i);
            if(b >= '0' && b <= '9'){
                pInt = pInt * 10 + (b-'0');
            }
            else{
                throw new ProtocolException("invalid character in length");
            }
            if(pInt > MAX_ALLOWED){
                throw new ProtocolException("exceeded max allowed length");
            }
        }
        return (int)pInt;
    }

}
