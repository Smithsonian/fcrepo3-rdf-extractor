
package edu.si.fcrepo.integration;

import static java.net.URI.create;
import static java.net.URLDecoder.decode;
import static java.net.URLEncoder.encode;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import org.akubraproject.map.IdMapper;
import org.slf4j.Logger;

/**
 * A simple {@link IdMapper} for use with integration tests.
 *
 * @author A. Soroka
 *
 */
public class TestIdMapper implements IdMapper {

    private static final Logger log = getLogger(TestIdMapper.class);

    @Override
    public URI getExternalId(final URI internalId) {
        try {
            return create("info:fedora/" + decode(internalId.toString(), "UTF-8"));
        } catch (final UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public URI getInternalId(final URI externalId) {
        try {
            log.debug("Received request for external ID: {}", externalId);
            final URI internal = create(encode(externalId.toString(), "UTF-8"));
            log.debug("Looking for internal ID: {}", internal);
            return internal;
        } catch (final UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public String getInternalPrefix(final String externalPrefix) {
        return null;
    }
}
