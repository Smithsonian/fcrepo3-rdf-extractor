package edu.si.fcrepo;

import static java.lang.Thread.currentThread;

import org.junit.Assert;
import org.junit.Test;

import ch.qos.logback.classic.spi.LoggingEvent;

public class ThreadNameDiscriminatorTest extends Assert {

    private ThreadNameDiscriminator testThreadNameDiscriminator = new ThreadNameDiscriminator();

    @Test
    public void testGetKey() {
        assertEquals("Got wrong logging key!", "threadName", testThreadNameDiscriminator.getKey());
    }

    @Test
    public void testGetDiscriminatingValue() throws InterruptedException {
        String discriminatingValue = testThreadNameDiscriminator.getDiscriminatingValue(new LoggingEvent());
        assertEquals("Got wrong thread name!", currentThread().getName(), discriminatingValue);
        Thread otherThread = new Thread(() -> {
            String otherDiscriminatingValue = testThreadNameDiscriminator.getDiscriminatingValue(new LoggingEvent());
            assertEquals("Got wrong thread name!", currentThread().getName(), otherDiscriminatingValue);
        });
        otherThread.run();
        otherThread.join();
    }
}
