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

package edu.si.fcrepo;

import static org.apache.jena.graph.NodeFactory.createLiteral;
import static org.apache.jena.graph.NodeFactory.createURI;

import org.apache.jena.graph.Node;

import com.github.cwilper.fcrepo.dto.core.ControlGroup;
import com.github.cwilper.fcrepo.dto.core.State;

/**
 * RDF vocabulary terms and convenience methods for selecting from them.
 *
 * @author A. Soroka
 */
public final class RdfVocabulary {

    public static final String SYSTEM_NS = "info:fedora/fedora-system:";

    public static final String MODEL_NS = SYSTEM_NS + "def/model#";

    public static final String VIEW_NS = SYSTEM_NS + "def/view#";

    public static final Node CREATEDDATE = createURI(MODEL_NS + "createdDate");

    public static final Node LASTMODIFIEDDATE = createURI(VIEW_NS + "lastModifiedDate");

    public static final Node LABEL = createURI(MODEL_NS + "label");

    public static final Node OWNER = createURI(MODEL_NS + "ownerId");

    public static final Node STATE = createURI(MODEL_NS + "state");

    public static final Node ACTIVE = createURI(MODEL_NS + "Active");

    public static final Node INACTIVE = createURI(MODEL_NS + "Inactive");

    public static final Node DELETED = createURI(MODEL_NS + "Deleted");

    public static final Node MIME_TYPE = createURI(VIEW_NS + "mimeType");

    public static final Node IS_VOLATILE = createURI(VIEW_NS + "isVolatile");

    public static final Node DISSEMINATES = createURI(VIEW_NS + "disseminates");

    public static final Node DISSEMINATION_TYPE = createURI(VIEW_NS + "disseminationType");

    public static final Node FALSE = createLiteral("false");

    public static final Node TRUE = createLiteral("true");

    public static final Node HAS_MODEL = createURI(MODEL_NS + "hasModel");

    public static final Node FEDORA_OBJECT = createURI(SYSTEM_NS + "FedoraObject-3.0");

    public static Node state(final State state) {
        switch (state) {
            case ACTIVE:
                return RdfVocabulary.ACTIVE;
            case INACTIVE:
                return RdfVocabulary.INACTIVE;
            case DELETED:
                return RdfVocabulary.DELETED;
            default:
                throw new IllegalArgumentException("Impossible object state: " + state);
        }
    }

    public static Node volatility(final ControlGroup controlGroup) {
        switch (controlGroup) {
            case INLINE_XML:
            case MANAGED:
                return FALSE;
            case REDIRECT:
            case EXTERNAL:
                return TRUE;
            default:
                throw new IllegalArgumentException("Impossible datastream control group value: " + controlGroup);
        }
    }

    private RdfVocabulary() {}

}
