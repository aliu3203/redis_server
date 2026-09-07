package server;

import java.util.Locale;
import java.nio.charset.StandardCharsets;

public class Command{
    private String name;
    private byte[][] argv;

    Command(byte[][] argv){
        this.argv = argv;
        if(argv.length == 0){
            name = "";
        }
        else{
            name = new String(argv[0], StandardCharsets.ISO_8859_1).toUpperCase(Locale.ROOT);
        }
        
    }
    public String name(){
        return this.name;
    }
    public int argc(){
        return argv.length;
    }
}
