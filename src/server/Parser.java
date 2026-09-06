package server;

public final class Parser{

    private Buffer buf;
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

    }

}
