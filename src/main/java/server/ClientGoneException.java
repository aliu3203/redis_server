package server;

// The client disconnected while blocked in BLPOP. Anything it was about to
// receive has already been handed back, so the connection should be closed
// without writing a reply.
public class ClientGoneException extends Exception{
    ClientGoneException(){
        super("client disconnected while blocked in BLPOP");
    }
}
