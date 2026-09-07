package server;

public class Command{
    private String name;
    private byte[][] argv;

    Command(byte[][] argv){
        this.argv = argv;
        name = argv[0];
    }
    public String name(){
        return this.name;
    }
    public int argc(){
        return argv.length;
    }
}
