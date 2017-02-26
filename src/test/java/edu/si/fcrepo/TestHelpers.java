
package edu.si.fcrepo;

import static java.lang.Thread.currentThread;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.InputStream;

import org.slf4j.Logger;

public class TestHelpers {

    private static final Logger log = getLogger(TestHelpers.class);

    private TestHelpers() {}



    public static InputStream loadResource(final String name) {
        return currentThread().getContextClassLoader().getResourceAsStream(name);
    }

}
