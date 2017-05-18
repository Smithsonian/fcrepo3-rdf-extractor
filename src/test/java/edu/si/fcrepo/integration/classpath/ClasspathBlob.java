/*
 * /*
 *  * Copyright 2015-2016 Smithsonian Institution.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  * use this file except in compliance with the License.You may obtain a copy of
 *  * the License at: http://www.apache.org/licenses/
 *  *
 *  * This software and accompanying documentation is supplied without
 *  * warranty of any kind. The copyright holder and the Smithsonian Institution:
 *  * (1) expressly disclaim any warranties, express or implied, including but not
 *  * limited to any implied warranties of merchantability, fitness for a
 *  * particular purpose, title or non-infringement; (2) do not assume any legal
 *  * liability or responsibility for the accuracy, completeness, or usefulness of
 *  * the software; (3) do not represent that use of the software would not
 *  * infringe privately owned rights; (4) do not warrant that the software
 *  * is error-free or will be maintained, supported, updated or enhanced;
 *  * (5) will not be liable for any indirect, incidental, consequential special
 *  * or punitive damages of any kind or nature, including but not limited to lost
 *  * profits or loss of data, on any basis arising from contract, tort or
 *  * otherwise, even if any of the parties has been warned of the possibility of
 *  * such loss or damage.
 *  *
 *  * This distribution includes several third-party libraries, each with their own
 *  * license terms. For a complete copy of all copyright and license terms, including
 *  * those of third-party libraries, please see the product release notes.
 *  *
 *  */
 */

package edu.si.fcrepo.integration.classpath;

import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;

import org.akubraproject.Blob;
import org.akubraproject.impl.AbstractBlob;
import org.slf4j.Logger;
import org.springframework.core.io.ClassPathResource;

public class ClasspathBlob extends AbstractBlob {

    protected final ClasspathBlobStoreConnection conn;

    private static final Logger log = getLogger(ClasspathBlob.class);

    protected ClasspathBlob(final ClasspathBlobStoreConnection owner, final URI id) {
        super(owner, id);
        this.conn = owner;
    }

    @Override
    public InputStream openInputStream() throws IOException {
        final String resourceLocation = conn.location + "/" + getId();
        log.debug("Retrieving resource from: {}", resourceLocation);
        return new ClassPathResource(resourceLocation).getInputStream();
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
