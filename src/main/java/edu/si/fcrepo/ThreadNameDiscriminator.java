package edu.si.fcrepo;

import static java.lang.Thread.currentThread;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.sift.Discriminator;

public class ThreadNameDiscriminator implements Discriminator<ILoggingEvent> {

    private static final String KEY = "threadName";

    private volatile boolean started;

    @Override
    public String getDiscriminatingValue(ILoggingEvent iLoggingEvent) {
        return currentThread().getName();
    }

    @Override
    public String getKey() {
        return KEY;
    }

    public void start() {
        started = true;
    }

    public void stop() {
        started = false;
    }

    public boolean isStarted() {
        return started;
    }
}