
package edu.si.fcrepo.integration.classpath;

import static org.slf4j.LoggerFactory.getLogger;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;

import org.akubraproject.Blob;
import org.akubraproject.impl.AbstractBlob;
import org.slf4j.Logger;
import edu.si.fcrepo.TestHelpers;

public class ClasspathBlob extends AbstractBlob {

    protected final ClasspathBlobStoreConnection conn;

    private static final Logger log = getLogger(ClasspathBlob.class);

    protected ClasspathBlob(final ClasspathBlobStoreConnection owner, final URI id) {
        super(owner, id);
        this.conn = owner;
    }

    @Override
    public InputStream openInputStream() {
        final String resourceLocation = conn.location + "/" + getId();
        log.debug("Retrieving resource from: {}", resourceLocation);
        return TestHelpers.loadResource(resourceLocation);
    }

    @Override
    public OutputStream openOutputStream(final long estimatedSize, final boolean overwrite) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getSize() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean exists() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Blob moveTo(final URI blobId, final Map<String, String> hints) {
        throw new UnsupportedOperationException();
    }

}
