package server;

public class WrongTypeException extends RuntimeException{
    WrongTypeException(){
        super("WRONGTYPE operation on key is wrong type");
    }
}
