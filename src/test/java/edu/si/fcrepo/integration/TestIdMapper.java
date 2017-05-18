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
