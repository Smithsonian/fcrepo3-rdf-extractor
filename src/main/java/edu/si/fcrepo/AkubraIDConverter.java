/*
 * Copyright 2017 Smithsonian Institution.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.You may obtain a copy of
 * the License at: http://www.apache.org/licenses/
 *
 * This software and accompanying documentation is supplied without
 * warranty of any kind. The copyright holder and the Smithsonian Institution:
 * (1) expressly disclaim any warranties, express or implied, including but not
 * limited to any implied warranties of merchantability, fitness for a
 * particular purpose, title or non-infringement; (2) do not assume any legal
 * liability or responsibility for the accuracy, completeness, or usefulness of
 * the software; (3) do not represent that use of the software would not
 * infringe privately owned rights; (4) do not warrant that the software
 * is error-free or will be maintained, supported, updated or enhanced;
 * (5) will not be liable for any indirect, incidental, consequential special
 * or punitive damages of any kind or nature, including but not limited to lost
 * profits or loss of data, on any basis arising from contract, tort or
 * otherwise, even if any of the parties has been warned of the possibility of
 * such loss or damage.
 *
 * This distribution includes several third-party libraries, each with their own
 * license terms. For a complete copy of all copyright and license terms, including
 * those of third-party libraries, please see the product release notes.
 */

package edu.si.fcrepo;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;

/**
 * Utility functions extracted/modified based on org.fcrepo.server.storage.lowlevel.akubra.AkubraLowLevelStorage.
 * Original code authored by Chris Wilper
 *
 * @author whikloj
 */
public class AkubraIDConverter {

    /**
     * Rather than bring in the entire PID class to get a string.
     */
    public static String FEDORA_URI = "info:fedora/";

    /**
     * Converts a token to a token-as-blobId.
     *
     * @param token the token to convert
     * @return the AkubraLowLevelStorage BlobId
     */
    public static URI getBlobId(final URI token) {
        return getBlobId(token.toString());
    }

    /**
     * Converts a token to a token-as-blobId.
     * <p>
     * Object tokens are simply prepended with <code>info:fedora/</code>, whereas datastream tokens are additionally
     * converted such that <code>ns:id+dsId+dsVersionId</code> becomes
     * <code>info:fedora/ns:id/dsId/dsVersionId</code>, with the dsId and dsVersionId segments URI-percent-encoded
     * with UTF-8 character encoding.
     *
     * @param token the token to convert.
     * @return the blob id.
     * @throws IllegalArgumentException if the token is not a well-formed pid or datastream token.
     */
    public static URI getBlobId(final String token) {
        try {
            final int i = token.indexOf('+');
            if (i == -1) {
                if (token.startsWith(FEDORA_URI)) {
                    // We have the namespace so we're good.
                    return new URI(token);
                }
                // Just prepend the namespace
                return new URI(FEDORA_URI + token);
            } else {
                final String[] dsParts = token.substring(i + 1).split("\\+");
                if (dsParts.length != 2) {
                    throw new IllegalArgumentException(
                            "Malformed datastream token: " + token);
                }
                try {
                    final String encodedToken = token.substring(0, i) + "/" + uriEncode(dsParts[0]) + "/" + uriEncode(
                            dsParts[1]);
                    if (token.startsWith(FEDORA_URI)) {
                        return new URI(encodedToken);
                    } else {
                        return new URI(FEDORA_URI + encodedToken);
                    }
                } catch (final UnsupportedEncodingException e) {
                    throw new IllegalArgumentException("Unsupported encoding", e);
                }
            }
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException(
                    "Malformed object or datastream token: " + token, e);
        }
    }

    private static String uriEncode(final String s) throws UnsupportedEncodingException {
        return URLEncoder.encode(s, "UTF-8");
    }
}
