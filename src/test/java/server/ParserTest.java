package server;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

// Parser and Buffer, fed bytes directly -- no sockets.
//
//   mvn test -Dtest=ParserTest
class ParserTest{

    // ------------------------------------------------------------------
    // multibulk happy paths
    // ------------------------------------------------------------------

    @Test
    void happyPathPing() throws Exception{
        Buffer b = new Buffer();
        feed(b, "*1\r\n$4\r\nPING\r\n");
        Command cmd = Parser.tryParse(b);

        assertNotNull(cmd, "parsed a command");
        assertEquals("PING", cmd.name(), "name");
        assertEquals(1, cmd.argc(), "argc");
        assertNull(Parser.tryParse(b), "buffer fully consumed");
    }

    @Test
    void multipleArguments() throws Exception{
        Buffer b = new Buffer();
        feed(b, "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n");
        Command cmd = Parser.tryParse(b);

        assertNotNull(cmd, "parsed a command");
        assertEquals("SET", cmd.name(), "name");
        assertEquals(3, cmd.argc(), "argc");
        assertEquals("mykey", str(cmd.arg(1)), "arg(1)");
        assertEquals("myvalue", str(cmd.arg(2)), "arg(2)");
    }

    @Test
    void lowercaseNameIsUppercased() throws Exception{
        Buffer b = new Buffer();
        feed(b, "*1\r\n$4\r\nping\r\n");
        Command cmd = Parser.tryParse(b);

        assertNotNull(cmd, "parsed a command");
        assertEquals("PING", cmd.name(), "name() uppercases");
        assertEquals("ping", str(cmd.arg(0)), "arg(0) keeps the original bytes");
    }

    // ------------------------------------------------------------------
    // incremental / streaming behaviour
    // ------------------------------------------------------------------

    // A command split across two reads must return null without consuming,
    // then parse cleanly once the rest arrives.
    @Test
    void splitDelivery() throws Exception{
        Buffer b = new Buffer();

        feed(b, "*1\r\n$4\r\nPI");
        assertNull(Parser.tryParse(b), "incomplete returns null");

        feed(b, "NG\r\n");
        Command cmd = Parser.tryParse(b);
        assertNotNull(cmd, "completes after the rest arrives");
        assertEquals("PING", cmd.name(), "name");
    }

    // Two commands delivered in one read must both come back, in order.
    @Test
    void pipelinedCommands() throws Exception{
        Buffer b = new Buffer();
        feed(b, "*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n");

        Command first = Parser.tryParse(b);
        assertNotNull(first, "first command");
        assertEquals("PING", first.name(), "first name");

        Command second = Parser.tryParse(b);
        assertNotNull(second, "second command");
        assertEquals("ECHO", second.name(), "second name");
        assertEquals("hi", str(second.arg(1)), "second arg");

        assertNull(Parser.tryParse(b), "nothing left");
    }

    // Header promises 2 args but only 1 is present: wait, don't error.
    @Test
    void incompleteMultibulkWaits() throws Exception{
        Buffer b = new Buffer();
        feed(b, "*2\r\n$4\r\nECHO\r\n");
        assertNull(Parser.tryParse(b), "returns null");

        feed(b, "$2\r\nhi\r\n");
        Command cmd = Parser.tryParse(b);
        assertNotNull(cmd, "completes later");
        assertEquals(2, cmd.argc(), "argc");
        assertEquals("hi", str(cmd.arg(1)), "arg(1)");
    }

    // ------------------------------------------------------------------
    // payload edge cases
    // ------------------------------------------------------------------

    // The whole point of a length prefix: payload may contain CRLF.
    @Test
    void binarySafePayload() throws Exception{
        Buffer b = new Buffer();
        feed(b, "*1\r\n$5\r\na\r\nbc\r\n");
        Command cmd = Parser.tryParse(b);

        assertNotNull(cmd, "parsed a command");
        assertEquals(1, cmd.argc(), "argc");
        assertEquals(5, cmd.arg(0).length, "payload length");
        assertEquals("a\r\nbc", str(cmd.arg(0)), "payload bytes");
        assertNull(Parser.tryParse(b), "nothing left over");
    }

    @Test
    void emptyBulkString() throws Exception{
        Buffer b = new Buffer();
        feed(b, "*1\r\n$0\r\n\r\n");
        Command cmd = Parser.tryParse(b);

        assertNotNull(cmd, "parsed a command");
        assertEquals(1, cmd.argc(), "argc");
        assertEquals(0, cmd.arg(0).length, "arg is zero length");
        assertNull(Parser.tryParse(b), "nothing left over");
    }

    @Test
    void emptyArray() throws Exception{
        Buffer b = new Buffer();
        feed(b, "*0\r\n");
        Command cmd = Parser.tryParse(b);

        assertNotNull(cmd, "parsed a command");
        assertEquals(0, cmd.argc(), "argc");
        assertEquals("", cmd.name(), "name is empty string");
        assertNull(Parser.tryParse(b), "all 4 bytes consumed");
    }

    // ------------------------------------------------------------------
    // inline commands
    // ------------------------------------------------------------------

    @Test
    void inlineSimple() throws Exception{
        Buffer b = new Buffer();
        feed(b, "PING\r\n");
        Command cmd = Parser.tryParse(b);

        assertNotNull(cmd, "parsed a command");
        assertEquals("PING", cmd.name(), "name");
        assertEquals(1, cmd.argc(), "argc");
    }

    @Test
    void inlineCollapsesRepeatedSpaces() throws Exception{
        Buffer b = new Buffer();
        feed(b, "ECHO   hi\r\n");
        Command cmd = Parser.tryParse(b);

        assertNotNull(cmd, "parsed a command");
        assertEquals(2, cmd.argc(), "argc (no empty tokens)");
        assertEquals("ECHO", cmd.name(), "name");
        assertEquals("hi", str(cmd.arg(1)), "arg(1)");
    }

    @Test
    void inlineLeadingAndTrailingSpaces() throws Exception{
        Buffer b = new Buffer();
        feed(b, "  ECHO hi  \r\n");
        Command cmd = Parser.tryParse(b);

        assertNotNull(cmd, "parsed a command");
        assertEquals(2, cmd.argc(), "argc");
        assertEquals("ECHO", cmd.name(), "name");
        assertEquals("hi", str(cmd.arg(1)), "arg(1)");
    }

    // ------------------------------------------------------------------
    // malformed input
    // ------------------------------------------------------------------

    @Test
    void errorBulkMissingDollar(){
        assertProtocolError("*1\r\n#4\r\nPING\r\n");
    }

    @Test
    void errorNonDigitInLength(){
        assertProtocolError("*x\r\n$4\r\nPING\r\n");
    }

    // Header says 3 bytes, payload is 4: the trailing CRLF check must catch it.
    @Test
    void errorLengthLiesAboutPayload(){
        assertProtocolError("*1\r\n$3\r\nPING\r\n");
    }

    // Real Redis treats *-1 as a null array; this parser rejects the '-'.
    // Pinning current behaviour so a future change is a deliberate one.
    @Test
    void errorNegativeCount(){
        assertProtocolError("*-1\r\n");
    }

    // ------------------------------------------------------------------
    // buffer mechanics
    // ------------------------------------------------------------------

    // Payload larger than INITIAL_CAPACITY forces Buffer to grow.
    @Test
    void bufferGrowsBeyondInitialCapacity() throws Exception{
        int size = 20000;
        String payload = "a".repeat(size);
        Buffer b = new Buffer();
        feed(b, "*2\r\n$3\r\nSET\r\n$" + size + "\r\n" + payload + "\r\n");

        Command cmd = Parser.tryParse(b);
        assertNotNull(cmd, "parsed a command");
        assertEquals(2, cmd.argc(), "argc");
        assertEquals(size, cmd.arg(1).length, "payload length");
        assertEquals((byte)'a', cmd.arg(1)[0], "first payload byte");
        assertEquals((byte)'a', cmd.arg(1)[size - 1], "last payload byte");
    }

    // Args must be independent copies, not views into the buffer, so that
    // consuming and compacting the buffer cannot corrupt an earlier Command.
    @Test
    void argsSurviveCompaction() throws Exception{
        Buffer b = new Buffer();
        feed(b, "*1\r\n$4\r\nPING\r\n");
        Command first = Parser.tryParse(b);
        assertNotNull(first, "first command parsed");
        byte[] heldArg = first.arg(0);

        // Push the buffer past its initial capacity, forcing the compaction
        // and growth paths in ensureSpace() to run underneath us.
        int size = 20000;
        feed(b, "*1\r\n$" + size + "\r\n" + "b".repeat(size) + "\r\n");
        Command second = Parser.tryParse(b);

        assertNotNull(second, "second command parsed");
        assertEquals(size, second.arg(0).length, "second payload length");
        assertEquals("PING", str(heldArg), "held arg unchanged");
        assertSame(heldArg, first.arg(0), "held arg is the same object");
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private static byte[] raw(String s){
        return s.getBytes(StandardCharsets.ISO_8859_1);
    }

    private static String str(byte[] b){
        return new String(b, StandardCharsets.ISO_8859_1);
    }

    // Mimics a socket delivering bytes: keeps calling Buffer.read until the
    // stream is drained, so Buffer decides its own chunk sizes.
    private static void feed(Buffer b, String wire) throws IOException{
        InputStream in = new ByteArrayInputStream(raw(wire));
        while(b.read(in) > 0){
            // keep reading
        }
    }

    private static void assertProtocolError(String wire){
        Buffer b = new Buffer();
        assertThrows(ProtocolError.class, () -> {
            feed(b, wire);
            Parser.tryParse(b);
        });
    }
}
