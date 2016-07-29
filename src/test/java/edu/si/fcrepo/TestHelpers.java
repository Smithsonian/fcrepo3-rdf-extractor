
package edu.si.fcrepo;

import static java.lang.Thread.currentThread;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import org.apache.jena.rdf.model.Model;
import org.slf4j.Logger;

public class TestHelpers {

    private static final Logger log = getLogger(TestHelpers.class);

    private TestHelpers() {}

    public static String uriForResource(final String name) {
        log.debug("Retrieving resource: {}", name);
        return currentThread().getContextClassLoader().getResource(name).toString();
    }

    public static InputStream loadResource(final String name) {
        return currentThread().getContextClassLoader().getResourceAsStream(name);
    }

    public static String ntriples(final Model m) {
        try (StringWriter w = new StringWriter()) {
            m.write(w, "N-TRIPLE");
            return w.toString();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

}
