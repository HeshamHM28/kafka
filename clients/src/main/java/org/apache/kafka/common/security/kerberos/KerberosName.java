/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.security.kerberos;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KerberosName {

    /**
     * A pattern that matches a Kerberos name with at most 3 components.
     */
    private static final Pattern NAME_PARSER = Pattern.compile("([^/@]*)(/([^/@]*))?@([^/@]*)");

    /** The first component of the name */
    private final String serviceName;
    /** The second component of the name. It may be null. */
    private final String hostName;
    /** The realm of the name. */
    private final String realm;

    /**
     * Creates an instance of `KerberosName` with the provided parameters.
     */
    public KerberosName(String serviceName, String hostName, String realm) {
        if (serviceName == null)
            throw new IllegalArgumentException("serviceName must not be null");
        this.serviceName = serviceName;
        this.hostName = hostName;
        this.realm = realm;
    }

    /**
     * Create a name from the full Kerberos principal name.
     */
    public static KerberosName parse(String principalName) {
        // Fast-path: if there is no '@', return the whole principal as the service name.
        int at = principalName.indexOf('@');
        if (at == -1) {
            return new KerberosName(principalName, null, null);
        }

        // Split into left (service[/host]) and realm parts.
        String left = principalName.substring(0, at);
        String realm = principalName.substring(at + 1);

        // Validate realm: it must not contain '@' or '/'.
        if (realm.indexOf('@') != -1 || realm.indexOf('/') != -1) {
            throw new IllegalArgumentException("Malformed Kerberos name: " + principalName);
        }

        // Parse left into service and optional host (only one slash allowed).
        int slash = left.indexOf('/');
        if (slash == -1) {
            return new KerberosName(left, null, realm);
        } else {
            // Reject if there is more than one slash.
            if (left.indexOf('/', slash + 1) != -1) {
                throw new IllegalArgumentException("Malformed Kerberos name: " + principalName);
            }
            String service = left.substring(0, slash);
            String host = left.substring(slash + 1);
            return new KerberosName(service, host, realm);
        }
    }

    /**
     * Put the name back together from the parts.
     */
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append(serviceName);
        if (hostName != null) {
            result.append('/');
            result.append(hostName);
        }
        if (realm != null) {
            result.append('@');
            result.append(realm);
        }
        return result.toString();
    }

    /**
     * Get the first component of the name.
     * @return the first section of the Kerberos principal name
     */
    public String serviceName() {
        return serviceName;
    }

    /**
     * Get the second component of the name.
     * @return the second section of the Kerberos principal name, and may be null
     */
    public String hostName() {
        return hostName;
    }

    /**
     * Get the realm of the name.
     * @return the realm of the name, may be null
     */
    public String realm() {
        return realm;
    }

}
