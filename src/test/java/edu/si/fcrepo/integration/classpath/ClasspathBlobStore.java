
package edu.si.fcrepo.integration.classpath;

import java.net.URI;
import java.util.Map;

import javax.transaction.Transaction;

import org.akubraproject.BlobStoreConnection;
import org.akubraproject.impl.AbstractBlobStore;

public class ClasspathBlobStore extends AbstractBlobStore {

    private final String location;

    public ClasspathBlobStore(final URI id, final String location) {
        super(id);
        this.location = location;
    }

    @Override
    public BlobStoreConnection openConnection(final Transaction tx, final Map<String, String> hints) {
        return new ClasspathBlobStoreConnection(this, location);
    }

}
