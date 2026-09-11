package server;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ParserTest{

    private static int checks = 0;
    private static int failures = 0;

    public static void main(String[] args){

        // --- multibulk happy paths ---
        run("happyPathPing", ParserTest::happyPathPing);
        run("multipleArguments", ParserTest::multipleArguments);
        run("lowercaseNameIsUppercased", ParserTest::lowercaseNameIsUppercased);

        // --- incremental / streaming behaviour ---
        run("splitDelivery", ParserTest::splitDelivery);
        run("pipelinedCommands", ParserTest::pipelinedCommands);
        run("incompleteMultibulkWaits", ParserTest::incompleteMultibulkWaits);

        // --- payload edge cases ---
        run("binarySafePayload", ParserTest::binarySafePayload);
        run("emptyBulkString", ParserTest::emptyBulkString);
        run("emptyArray", ParserTest::emptyArray);

        // --- inline commands ---
        run("inlineSimple", ParserTest::inlineSimple);
        run("inlineCollapsesRepeatedSpaces", ParserTest::inlineCollapsesRepeatedSpaces);
        run("inlineLeadingAndTrailingSpaces", ParserTest::inlineLeadingAndTrailingSpaces);

        // --- malformed input ---
        run("errorBulkMissingDollar", ParserTest::errorBulkMissingDollar);
        run("errorNonDigitInLength", ParserTest::errorNonDigitInLength);
        run("errorLengthLiesAboutPayload", ParserTest::errorLengthLiesAboutPayload);
        run("errorNegativeCount", ParserTest::errorNegativeCount);

        // --- buffer mechanics ---
        run("bufferGrowsBeyondInitialCapacity", ParserTest::bufferGrowsBeyondInitialCapacity);
        run("argsSurviveCompaction", ParserTest::argsSurviveCompaction);

        System.out.println();
        System.out.println(checks + " checks, " + failures + " failed");
        if(failures > 0){
            System.exit(1);
        }
    }

    private interface Testable{
        void run() throws Exception;
    }

    // Runs one test in isolation. An unexpected exception fails that test
    // and the run continues, so a single crash cannot hide the rest.
    private static void run(String name, Testable t){
        try{
            t.run();
        }
        catch(Throwable e){
            fail(name + ": threw " + e);
        }
    }

    // ------------------------------------------------------------------
    // tests
    // ------------------------------------------------------------------

    private static void happyPathPing() throws Exception{
        Buffer b = new Buffer();
        feed(b, "*1\r\n$4\r\nPING\r\n");
        Command cmd = Parser.tryParse(b);

        checkNotNull("happyPathPing: parsed a command", cmd);
        checkEquals("happyPathPing: name", "PING", cmd.name());
        checkEquals("happyPathPing: argc", 1, cmd.argc());
        checkEquals("happyPathPing: buffer fully consumed", null, Parser.tryParse(b));
    }

    private static void multipleArguments() throws Exception{
        Buffer b = new Buffer();
        feed(b, "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n");
        Command cmd = Parser.tryParse(b);

        checkNotNull("multipleArguments: parsed a command", cmd);
        checkEquals("multipleArguments: name", "SET", cmd.name());
        checkEquals("multipleArguments: argc", 3, cmd.argc());
        checkEquals("multipleArguments: arg(1)", "mykey", str(cmd.arg(1)));
        checkEquals("multipleArguments: arg(2)", "myvalue", str(cmd.arg(2)));
    }

    private static void lowercaseNameIsUppercased() throws Exception{
        Buffer b = new Buffer();
        feed(b, "*1\r\n$4\r\nping\r\n");
        Command cmd = Parser.tryParse(b);

        checkNotNull("lowercaseName: parsed a command", cmd);
        checkEquals("lowercaseName: name() uppercases", "PING", cmd.name());
        checkEquals("lowercaseName: arg(0) keeps original bytes", "ping", str(cmd.arg(0)));
    }

    // A command split across two reads must return null without consuming,
    // then parse cleanly once the rest arrives.
    private static void splitDelivery() throws Exception{
        Buffer b = new Buffer();

        feed(b, "*1\r\n$4\r\nPI");
        checkEquals("splitDelivery: incomplete returns null", null, Parser.tryParse(b));

        feed(b, "NG\r\n");
        Command cmd = Parser.tryParse(b);
        checkNotNull("splitDelivery: completes after rest arrives", cmd);
        checkEquals("splitDelivery: name", "PING", cmd.name());
    }

    // Two commands delivered in one read must both come back, in order.
    private static void pipelinedCommands() throws Exception{
        Buffer b = new Buffer();
        feed(b, "*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n");

        Command first = Parser.tryParse(b);
        checkNotNull("pipelined: first command", first);
        checkEquals("pipelined: first name", "PING", first.name());

        Command second = Parser.tryParse(b);
        checkNotNull("pipelined: second command", second);
        checkEquals("pipelined: second name", "ECHO", second.name());
        checkEquals("pipelined: second arg", "hi", str(second.arg(1)));

        checkEquals("pipelined: nothing left", null, Parser.tryParse(b));
    }

    // Header promises 2 args but only 1 is present: wait, don't error.
    private static void incompleteMultibulkWaits() throws Exception{
        Buffer b = new Buffer();
        feed(b, "*2\r\n$4\r\nECHO\r\n");
        checkEquals("incompleteMultibulk: returns null", null, Parser.tryParse(b));

        feed(b, "$2\r\nhi\r\n");
        Command cmd = Parser.tryParse(b);
        checkNotNull("incompleteMultibulk: completes later", cmd);
        checkEquals("incompleteMultibulk: argc", 2, cmd.argc());
        checkEquals("incompleteMultibulk: arg(1)", "hi", str(cmd.arg(1)));
    }

    // The whole point of a length prefix: payload may contain CRLF.
    private static void binarySafePayload() throws Exception{
        Buffer b = new Buffer();
        feed(b, "*1\r\n$5\r\na\r\nbc\r\n");
        Command cmd = Parser.tryParse(b);

        checkNotNull("binarySafe: parsed a command", cmd);
        checkEquals("binarySafe: argc", 1, cmd.argc());
        checkEquals("binarySafe: payload length", 5, cmd.arg(0).length);
        checkEquals("binarySafe: payload bytes", "a\r\nbc", str(cmd.arg(0)));
        checkEquals("binarySafe: nothing left over", null, Parser.tryParse(b));
    }

    private static void emptyBulkString() throws Exception{
        Buffer b = new Buffer();
        feed(b, "*1\r\n$0\r\n\r\n");
        Command cmd = Parser.tryParse(b);

        checkNotNull("emptyBulk: parsed a command", cmd);
        checkEquals("emptyBulk: argc", 1, cmd.argc());
        checkEquals("emptyBulk: arg is zero length", 0, cmd.arg(0).length);
        checkEquals("emptyBulk: nothing left over", null, Parser.tryParse(b));
    }

    private static void emptyArray() throws Exception{
        Buffer b = new Buffer();
        feed(b, "*0\r\n");
        Command cmd = Parser.tryParse(b);

        checkNotNull("emptyArray: parsed a command", cmd);
        checkEquals("emptyArray: argc", 0, cmd.argc());
        checkEquals("emptyArray: name is empty string", "", cmd.name());
        checkEquals("emptyArray: 4 bytes consumed", null, Parser.tryParse(b));
    }

    private static void inlineSimple() throws Exception{
        Buffer b = new Buffer();
        feed(b, "PING\r\n");
        Command cmd = Parser.tryParse(b);

        checkNotNull("inlineSimple: parsed a command", cmd);
        checkEquals("inlineSimple: name", "PING", cmd.name());
        checkEquals("inlineSimple: argc", 1, cmd.argc());
    }

    private static void inlineCollapsesRepeatedSpaces() throws Exception{
        Buffer b = new Buffer();
        feed(b, "ECHO   hi\r\n");
        Command cmd = Parser.tryParse(b);

        checkNotNull("inlineSpaces: parsed a command", cmd);
        checkEquals("inlineSpaces: argc (no empty tokens)", 2, cmd.argc());
        checkEquals("inlineSpaces: name", "ECHO", cmd.name());
        checkEquals("inlineSpaces: arg(1)", "hi", str(cmd.arg(1)));
    }

    private static void inlineLeadingAndTrailingSpaces() throws Exception{
        Buffer b = new Buffer();
        feed(b, "  ECHO hi  \r\n");
        Command cmd = Parser.tryParse(b);

        checkNotNull("inlinePadding: parsed a command", cmd);
        checkEquals("inlinePadding: argc", 2, cmd.argc());
        checkEquals("inlinePadding: name", "ECHO", cmd.name());
        checkEquals("inlinePadding: arg(1)", "hi", str(cmd.arg(1)));
    }

    private static void errorBulkMissingDollar() throws Exception{
        expectProtocolError("errorMissingDollar", "*1\r\n#4\r\nPING\r\n");
    }

    private static void errorNonDigitInLength() throws Exception{
        expectProtocolError("errorNonDigitLength", "*x\r\n$4\r\nPING\r\n");
    }

    // Header says 3 bytes, payload is 4: the trailing CRLF check must catch it.
    private static void errorLengthLiesAboutPayload() throws Exception{
        expectProtocolError("errorLyingLength", "*1\r\n$3\r\nPING\r\n");
    }

    // Real Redis treats *-1 as a null array; this parser rejects the '-'.
    // Pinning current behaviour so a future change is a deliberate one.
    private static void errorNegativeCount() throws Exception{
        expectProtocolError("errorNegativeCount", "*-1\r\n");
    }

    // Payload larger than INITIAL_CAPACITY forces Buffer to grow.
    private static void bufferGrowsBeyondInitialCapacity() throws Exception{
        int size = 20000;
        String payload = "a".repeat(size);
        Buffer b = new Buffer();
        feed(b, "*2\r\n$3\r\nSET\r\n$" + size + "\r\n" + payload + "\r\n");

        Command cmd = Parser.tryParse(b);
        checkNotNull("bufferGrowth: parsed a command", cmd);
        checkEquals("bufferGrowth: argc", 2, cmd.argc());
        checkEquals("bufferGrowth: payload length", size, cmd.arg(1).length);
        checkEquals("bufferGrowth: first payload byte", (byte)'a', cmd.arg(1)[0]);
        checkEquals("bufferGrowth: last payload byte", (byte)'a', cmd.arg(1)[size-1]);
    }

    // Args must be independent copies, not views into the buffer, so that
    // consuming and compacting the buffer cannot corrupt an earlier Command.
    private static void argsSurviveCompaction() throws Exception{
        Buffer b = new Buffer();
        feed(b, "*1\r\n$4\r\nPING\r\n");
        Command first = Parser.tryParse(b);
        checkNotNull("compaction: first command parsed", first);
        byte[] heldArg = first.arg(0);

        // Push the buffer past its initial capacity, forcing the compaction
        // and growth paths in ensureSpace() to run underneath us.
        int size = 20000;
        feed(b, "*1\r\n$" + size + "\r\n" + "b".repeat(size) + "\r\n");
        Command second = Parser.tryParse(b);

        checkNotNull("compaction: second command parsed", second);
        checkEquals("compaction: second payload length", size, second.arg(0).length);
        checkEquals("compaction: held arg unchanged", "PING", str(heldArg));
        checkEquals("compaction: held arg is same object", true, heldArg == first.arg(0));
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

    private static void expectProtocolError(String label, String wire){
        Buffer b = new Buffer();
        try{
            feed(b, wire);
            Command cmd = Parser.tryParse(b);
            fail(label + ": expected ProtocolError, got " + describe(cmd));
        }
        catch(ProtocolError e){
            pass(label + " -> ProtocolError(\"" + e.getMessage() + "\")");
        }
        catch(IOException e){
            fail(label + ": unexpected IOException " + e);
        }
    }

    private static String describe(Command cmd){
        if(cmd == null){
            return "null (parser waiting for more input)";
        }
        return "Command " + cmd.name() + " with argc " + cmd.argc();
    }

    private static void checkEquals(String label, Object expected, Object actual){
        boolean ok = (expected == null) ? (actual == null) : expected.equals(actual);
        if(ok){
            pass(label);
        }
        else{
            fail(label + ": expected <" + expected + "> but was <" + actual + ">");
        }
    }

    private static void checkNotNull(String label, Object actual){
        if(actual != null){
            pass(label);
        }
        else{
            fail(label + ": expected non-null, was null");
        }
    }

    private static void pass(String label){
        checks++;
        System.out.println("  ok   " + label);
    }

    private static void fail(String label){
        checks++;
        failures++;
        System.out.println("  FAIL " + label);
    }
}
