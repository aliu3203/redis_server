package server;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.fail;

// Assertions shared by the test classes. Lives under src/test, so it is only
// compiled for tests and never ends up in the server itself.
final class TestAsserts{

    private TestAsserts(){}

    // A BLPOP result must be exactly (key, value).
    static void assertPopped(String key, byte[] value, Popped p){
        if(p == null || !p.key().equals(key) || !Arrays.equals(p.value(), value)){
            fail("expected (" + key + ", " + str(value) + ") but was "
                 + (p == null ? "null" : "(" + p.key() + ", " + str(p.value()) + ")"));
        }
    }

    static void assertPopped(String key, String value, Popped p){
        assertPopped(key, value.getBytes(StandardCharsets.ISO_8859_1), p);
    }

    private static String str(byte[] b){
        return new String(b, StandardCharsets.ISO_8859_1);
    }
}
