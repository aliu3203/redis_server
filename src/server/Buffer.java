package server;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

public class Buffer{

    private static final int INITIAL_CAPACITY = 16384;
    private static final int MAX_CAPACITY = 1024*INITIAL_CAPACITY;

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
        if(writePos < buf.length){
            return;
        }
        if(buf.length >= MAX_CAPACITY){
            throw new IOException("command too large");
        }
        int newCap = (int) Math.min(MAX_CAPACITY, 2L * (long)buf.length);
        buf = Arrays.copyOf(buf, newCap);
    }



    public void consume(int pos){
        // Should never happen
        if(readPos + pos > writePos){
            throw new IllegalStateException("readPos was greater than writePos following consumption");
        }
        readPos += pos;
        
    }

    public byte at(int pos){
        int bufPos = readPos + pos;
        if(bufPos < readPos || bufPos >= writePos){
            throw new IndexOutOfBoundsException();
        }
        return buf[bufPos];
    }

    public int readableBytes(int from){
        // Out of bounds from
        if(from < 0 || from > writePos-readPos){
            throw new IndexOutOfBoundsException();
        }
        return writePos-readPos-from;
    }

    // returns index of first \r relative to readPos
    public int findCRLF(int from){
        int pos = from + readPos;
        if(!(pos >= readPos && pos <= writePos)){
            throw new IndexOutOfBoundsException();
        }
        for(int p = pos; p < writePos-1; p++){
            if(buf[p] == '\r' && buf[p+1] == '\n'){
                return p-readPos;
            }
        }
        return -1;
    }

}
