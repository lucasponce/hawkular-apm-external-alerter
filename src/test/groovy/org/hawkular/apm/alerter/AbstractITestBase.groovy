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
package org.hawkular.apm.alerter

import groovyx.net.http.ContentType
import groovyx.net.http.RESTClient
import org.junit.BeforeClass

import java.util.concurrent.atomic.AtomicInteger

/**
 * Base class for REST tests.
 *
 * @author Lucas Ponce
 */
class AbstractITestBase {

    static alerterURI = System.getProperty('hawkular.base-uri') ?: 'http://127.0.0.1:8080/hawkular/apm/alerter/'
    static alertsURI = System.getProperty('hawkular.base-uri') ?: 'http://127.0.0.1:8080/hawkular/alerts/'
    static RESTClient alerts, alerter
    static testTenant = "28026b36-8fe4-4332-84c8-524e173a68bf"

    @BeforeClass
    static void initClient() {

        alerter = new RESTClient(alerterURI, ContentType.JSON)
        alerter.handler.failure = { it }

        alerts = new RESTClient(alertsURI, ContentType.JSON)
        alerts.handler.failure = { it }

        alerter.headers.put("Hawkular-Tenant", testTenant)
        alerts.headers.put("Hawkular-Tenant", testTenant)
    }
}
