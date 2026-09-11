package server;

public class WrongTypeException extends RuntimeException{

    public static final String MESSAGE =
        "WRONGTYPE Operation against a key holding the wrong kind of value";

    WrongTypeException(){
        super(MESSAGE);
    }
}
