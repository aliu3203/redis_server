package server;

public class Command{
    private String name;
    private byte[][] argv;

    Command(int count){
        argv = new byte[count][];
        name = "";
    }
    public String name(){
        return this.name;
    }
    public int argc(){
        return argv.length();
    }
}
