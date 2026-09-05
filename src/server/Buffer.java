package server;

import java.io.*;
import java.net.*;

public class Buffer{

    private static final int INITIAL_CAPACITY = 256;

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
        int n = in.read(buf, writePos, buf.length-writePos);

        // advance pos by # of bytes read
        writePos += n;
        return n;
    }
}
