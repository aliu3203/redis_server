package server;

public class Command{
    private String name;
    private byte[][] argv;

    public String name(){
        return this.name;
    }
    public int argc(){
        return argv.length();
    }
}
