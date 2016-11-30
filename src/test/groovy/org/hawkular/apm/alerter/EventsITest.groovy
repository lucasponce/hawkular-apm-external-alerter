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

import org.hawkular.alerts.api.model.event.Event
import org.junit.Test

import static org.junit.Assert.assertEquals

/**
 * Events REST tests.
 *
 * @author Lucas Ponce
 */
class EventsITest extends AbstractITestBase {

    String uuid() {
        return UUID.randomUUID().toString()
    }

    Collection<Event> marketingScenario() {
        // User1 buys 5 times in < 10 seconds
        def now = System.currentTimeMillis()
        Event e1 = new Event(testTenant, uuid(), now, "apm-data-id", "TraceCompletion", "Buy Book")
        e1.addTag("duration", "1000")
        e1.addTag("accountId", "user1")
        Event e2 = new Event(testTenant, uuid(), now + 1000, "apm-data-id", "TraceCompletion", "Buy Music")
        e2.addTag("duration", "2000")
        e2.addTag("accountId", "user1")
        Event e3 = new Event(testTenant, uuid(), now + 2000, "apm-data-id", "TraceCompletion", "Buy Groceries")
        e3.addTag("duration", "1500")
        e3.addTag("accountId", "user1")
        Event e4 = new Event(testTenant, uuid(), now + 3000, "apm-data-id", "TraceCompletion", "Buy VideoGames")
        e4.addTag("duration", "3000")
        e4.addTag("accountId", "user1")
        Event e5 = new Event(testTenant, uuid(), now + 4000, "apm-data-id", "TraceCompletion", "Buy VideoGames")
        e5.addTag("duration", "3000")
        e5.addTag("accountId", "user1")

        // User2 buys 3 times > 10 seconds
        Event e6 = new Event(testTenant, uuid(), now, "apm-data-id", "TraceCompletion", "Buy Book")
        e6.addTag("duration", "1000")
        e6.addTag("accountId", "user2")
        Event e7 = new Event(testTenant, uuid(), now + 5000, "apm-data-id", "TraceCompletion", "Buy Music")
        e7.addTag("duration", "2000")
        e7.addTag("accountId", "user2")
        Event e8 = new Event(testTenant, uuid(), now + 10000, "apm-data-id", "TraceCompletion", "Buy Groceries")
        e8.addTag("duration", "1500")
        e8.addTag("accountId", "user2")

        return Arrays.asList(e1, e2, e3, e4, e5, e6, e7, e8);
    }


    @Test
    void sendAndNoPersistEvents() {
        def resp = alerter.post(path: "events/data", body: marketingScenario() )
        assertEquals(200, resp.status)
    }
}
