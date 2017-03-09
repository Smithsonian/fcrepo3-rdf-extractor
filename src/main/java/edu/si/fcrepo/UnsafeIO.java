package edu.si.fcrepo;

import java.io.IOException;

import org.apache.jena.atlas.RuntimeIOException;

/**
 * IOException => RuntimeIOException
 */
@FunctionalInterface
public interface UnsafeIO<T> {

    T call() throws IOException;

    static <U> U unsafeIO(UnsafeIO<U> u) {
        try {
            return u.call();
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }
}