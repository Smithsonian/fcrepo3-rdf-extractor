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

package org.fcrepo.server.storage.lowlevel.akubra;

import java.io.UnsupportedEncodingException;

import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;

import org.akubraproject.map.IdMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twmacinta.util.MD5;

/**
 * Provides a hash-based <code>file:</code> mapping for any URI.
 * <p>
 * The path component of each internal URI is derived from an MD5 hash of the external URI. The filename component is a
 * reversible encoding of the external URI that is safe to use as a filename on modern filesystems.
 * <p>
 * <h2>Hash Path Patterns</h2> The pattern given at construction time determines how the path component of each internal
 * URI will be composed. Within the pattern, the # character is a stand-in for a hexadecimal [0-f] digit from the MD5
 * hash of the external id.
 * <p>
 * Patterns:
 * <ul>
 * <li>must consist only of # and / characters.</li>
 * <li>must contain between 1 and 32 # characters.</li>
 * <li>must not begin or end with the / character.</li>
 * <li>must not contain consecutive / characters.</li>
 * </ul>
 * <p>
 * Example patterns:
 * <ul>
 * <li>Good: #</li>
 * <li>Good: ##/#</li>
 * <li>Good: ##/##/##</li>
 * <li>Bad: a</li>
 * <li>Bad: ##/</li>
 * <li>Bad: ##//##</li>
 * </ul>
 * <p>
 * <h2>Filesystem-Safe Encoding</h2> The last part of the internal URI is a "filesystem-safe" encoding of the external
 * URI. All characters will be UTF-8 percent-encoded ("URI escaped") except for the following:
 * <code>a-z A-Z 0-9 = ( ) [ ] -</code> In addition, <code>.</code> (period) will be escaped as <code>%2E</code> when it
 * occurs as the last character of the URI.
 * <p>
 * <h2>Example Mappings</h2> With pattern <em>#/#</em>:
 * <ul>
 * <li><code>urn:example1</code> becomes <code>file:0/8/urn%3Aexample1</code></li>
 * <li><code>http://tinyurl.com/cxzzf</code> becomes <code>file:6/2/http%3A%2F%2Ftinyurl.com%2Fcxzzf</code></li>
 * </ul>
 * With pattern <em>##/##</em>:
 * <ul>
 * <li><code>urn:example1</code> becomes <code>file:08/86/urn%3Aexample1</code></li>
 * <li><code>http://tinyurl.com/cxzzf</code> becomes <code>file:62/ca/http%3A%2F%2Ftinyurl.com%2Fcxzzf</code></li>
 * </ul>
 *
 * @author Chris Wilper
 * @author A. Soroka
 */
public class HashPathIdMapper implements IdMapper {
    
    private static final Logger LOG = LoggerFactory.getLogger(HashPathIdMapper.class);

    private static final String internalScheme = "file";

    private final String pattern;

    /**
     * Creates an instance that will use the given pattern.
     *
     * @param pattern the pattern to use, possibly <code>null</code> or "".
     * @throws IllegalArgumentException if the pattern is invalid.
     */
    public HashPathIdMapper(final String pattern) {
        this.pattern = validatePattern(pattern);
    }

    @Override
    public URI getExternalId(final URI internalId) throws NullPointerException {
        final String fullPath = internalId.toString().substring(internalScheme.length() + 1);
        final int i = fullPath.lastIndexOf('/');
        String encodedURI;
        if (i == -1) encodedURI = fullPath;
        else encodedURI = fullPath.substring(i + 1);
        return URI.create(decode(encodedURI));
    }

    @Override
    public URI getInternalId(final URI externalId) throws NullPointerException {
        if (externalId == null) throw new NullPointerException();
        final String uri = externalId.toString();
        final StringBuilder buffer = new StringBuilder(uri.length() + 16);
        buffer.append(internalScheme + ":");
        getPath(uri, buffer);
        encode(uri, buffer);
        String internalId = buffer.toString();
        LOG.debug("Accessing internal URI: {}", internalId);
        return URI.create(internalId);
    }

    //@Override
    @Override
    public String getInternalPrefix(final String externalPrefix) throws NullPointerException {
        if (externalPrefix == null) throw new NullPointerException();
        // we can only do this if pattern is ""
        if (pattern.length() == 0) {
            final StringBuilder buffer = new StringBuilder(externalPrefix.length() + 16);
            buffer.append(internalScheme + ":");
            encode(externalPrefix, buffer);
            return buffer.toString();
        }
        return null;
    }

    // gets the path based on the hash of the uri, or "" if the pattern is empty
    private void getPath(final String uri, final StringBuilder builder) {
        if (pattern.length() == 0) return;
        final String hash = getHash(uri);
        int hashPos = 0;
        for (int i = 0; i < pattern.length(); i++) {
            final char c = pattern.charAt(i);
            if (c == '#') builder.append(hash.charAt(hashPos++));
            else builder.append(c);
        }
        builder.append('/');
    }

    // computes the md5 and returns a 32-char lowercase hex string
    private static String getHash(final String uri) {
        return MD5.asHex(new MD5(uri).Final());
    }

    private static void encode(final String uri, final StringBuilder out) {
        // encode char-by-char because we only want to borrow
        // URLEncoder.encode's behavior for some characters
        for (int i = 0; i < uri.length(); i++) {
            final char c = uri.charAt(i);
            if (c >= 'a' && c <= 'z') out.append(c);
            else if (c >= '0' && c <= '9') out.append(c);
            else if (c >= 'A' && c <= 'Z') out.append(c);
            else if (c == '-' || c == '=' || c == '(' || c == ')' || c == '[' || c == ']' || c == ';') out.append(c);
            else if (c == ':') out.append("%3A");
            else if (c == ' ') out.append("%20");
            else if (c == '+') out.append("%2B");
            else if (c == '_') out.append("%5F");
            else if (c == '*') out.append("%2A");
            else if (c == '.') {
                if (i == uri.length() - 1) out.append("%2E");
                else out.append('.');
            } else try {
                out.append(URLEncoder.encode(Character.toString(c), "UTF-8"));
            } catch (final UnsupportedEncodingException wontHappen) {
                throw new RuntimeException(wontHappen);
            }
        }
    }

    private static String decode(final String encodedURI) {
        final String trimmedUri = encodedURI.endsWith("%2E")
                        ? encodedURI.substring(0, encodedURI.length() - 3).concat(".") : encodedURI;
        try {
            return URLDecoder.decode(trimmedUri, "UTF-8");
        } catch (final UnsupportedEncodingException wontHappen) {
            throw new RuntimeException(wontHappen);
        }
    }

    private static String validatePattern(final String pattern) {
        if (pattern == null) return "";
        int count = 0;
        boolean prevWasSlash = false;
        for (int i = 0; i < pattern.length(); i++) {
            final char c = pattern.charAt(i);
            if (c == '#') {
                count++;
                prevWasSlash = false;
            } else if (c == '/') {
                if (i == 0 || i == pattern.length() - 1)
                    throw new IllegalArgumentException("Pattern must not begin" + " or end with '/'");
                else if (prevWasSlash)
                    throw new IllegalArgumentException("Pattern must not" + " contain consecutive '/' characters");
                else prevWasSlash = true;
            } else throw new IllegalArgumentException("Illegal character in" + " pattern: " + c);
        }
        if (count > 32) throw new IllegalArgumentException("Pattern must not contain more" + " than 32 '#' characters");
        return pattern;
    }
}
