/*
 * Copyright 2015-2016 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkular.apm.alerter;

import java.util.HashMap;
import java.util.Map;

import javax.naming.InitialContext;

/**
 * Helper class to determine the JNDI name of a specific service
 *
 * @author Jay Shaughnessy
 * @author Lucas Ponce
 *
 */
public class ServiceNames {

    public static String HAWKULAR_ALERTS_ACTIONS_ENV = "hawkular-alerts.alerter-deployment";
    public static String STANDALONE = "standalone";

    private static String JNDI_ACTIONS_METRICS = "java:global/hawkular-metrics/hawkular-alerts/CassActionsServiceImpl";
    private static String JNDI_ACTIONS_STANDALONE = "java:global/hawkular-alerts/CassActionsServiceImpl";
    private static String JNDI_DEFINITIONS_METRICS = "java:global/hawkular-metrics/hawkular-alerts/CassDefinitionsServiceImpl";
    private static String JNDI_DEFINITIONS_STANDALONE = "java:global/hawkular-alerts/CassDefinitionsServiceImpl";

    public enum Service {
        ACTIONS_SERVICE, DEFINITIONS_SERVICE
    }

    private static Map<Service, String> services;

    static {
        services = new HashMap<>();
        String env = null;
        try {
            env = (String) new InitialContext().lookup("java:comp/env/" + HAWKULAR_ALERTS_ACTIONS_ENV);
        } catch (Exception e) {
            // env does not need to be set, we'll default to METRICS
        }
        if (STANDALONE.equals(env)) {
            services.put(Service.ACTIONS_SERVICE, JNDI_ACTIONS_STANDALONE);
            services.put(Service.DEFINITIONS_SERVICE, JNDI_DEFINITIONS_STANDALONE);
        } else {
            services.put(Service.ACTIONS_SERVICE, JNDI_ACTIONS_METRICS);
            services.put(Service.DEFINITIONS_SERVICE, JNDI_DEFINITIONS_METRICS);
        }
    }

    public static String getServiceName(Service service) {
        return services.get(service);
    }
}
