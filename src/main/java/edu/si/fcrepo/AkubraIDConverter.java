package edu.si.fcrepo;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;

/**
 * Utility functions extracted from: org.fcrepo.server.storage.lowlevel.akubra.AkubraLowLevelStorage Originally
 * authored by
 *
 * @author Chris Wilper
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
