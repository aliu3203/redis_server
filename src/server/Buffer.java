package server;

import java.io.*;
import java.net.*;

public class Buffer{

    private static final int INITIAL_CAPACITY = 16384;
    private static final int MAX_CAPACITY = 64*INITIAL_CAPACITY;

    private byte[] buf;

    // start writing at writePos
    // readPos = what position we've processed to, everything after is unprocessed
    private int writePos;
    private int readPos;

    Buffer(){
        buf = new byte[INITIAL_CAPACITY];
        writePos = 0;
        readPos = 0;
    }

    public int read(InputStream in) throws IOException{
        ensureSpace();
        int n = in.read(buf, writePos, buf.length-writePos);
        // advance pos by # of bytes read
        if(n > 0){
            writePos += n;
        }
        return n;
    }

    private void ensureSpace() throws IOException{
        if(writePos < buf.length){
            return;
        }
        if(readPos > 0){
            System.arraycopy(buf, readPos, buf, 0, writePos-readPos);
            writePos -= readPos;
            readPos = 0;
        }
        if(writePos == buf.length){
            // Eventually want to expand until MAX_CAPACITY by copying array and doubling it
            // For now simply throw exception
            throw new IOException("command too large");
        }
    }
}
