package server;

public class ProtocolError extends Exception{
    ProtocolError(String msg){
        super(msg);
    }
}
