
package edu.si.fcrepo.integration.classpath;

import static edu.si.fcrepo.UnsafeIO.unsafeIO;
import static java.util.Arrays.stream;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;

import org.akubraproject.Blob;
import org.akubraproject.impl.AbstractBlobStoreConnection;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

public class ClasspathBlobStoreConnection extends AbstractBlobStoreConnection {

    final String location;

    public ClasspathBlobStoreConnection(final ClasspathBlobStore store, final String location) {
        super(store);
        this.location = location;
    }

    @Override
    public Blob getBlob(final URI blobId, final Map<String, String> hints) {
        return new ClasspathBlob(this, blobId);
    }

    @Override
    public Iterator<URI> listBlobIds(final String filterPrefix) throws IOException {
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        return stream(resolver.getResources(location)).map(u -> unsafeIO(() -> u.getURI())).iterator();
    }

    @Override
    public void sync() {
        throw new UnsupportedOperationException();
    }
}
