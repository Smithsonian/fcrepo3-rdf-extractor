package edu.si.fcrepo;

import static java.lang.Thread.currentThread;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.sift.AbstractDiscriminator;

/**
 * Convenient for sifting Logback logs by thread.
 * 
 * @author ajs6f
 *
 */
public class ThreadNameDiscriminator extends AbstractDiscriminator<ILoggingEvent> {

    private static final String KEY = "threadName";

    @Override
    public String getDiscriminatingValue(ILoggingEvent iLoggingEvent) {
        return currentThread().getName();
    }

    @Override
    public String getKey() {
        return KEY;
    }
}