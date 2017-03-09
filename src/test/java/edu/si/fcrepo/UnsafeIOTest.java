package edu.si.fcrepo;

import java.io.IOException;

import org.apache.jena.atlas.RuntimeIOException;
import org.junit.Assert;
import org.junit.Test;

public class UnsafeIOTest extends Assert {

    @Test
    public void test() {
        IOException ioException = new IOException();
        try {
            UnsafeIO.unsafeIO(() -> {
                throw ioException;
            });
            fail("Should have thrown runtime exception instead!");
        } catch (RuntimeIOException e) {
            assertSame("Got wrong runtime exception!", ioException, e.getCause());
        }
    }
}
