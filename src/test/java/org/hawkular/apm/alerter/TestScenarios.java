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

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import org.hawkular.alerts.api.model.event.Event;

/**
 * @author Jay Shaughnessy
 * @author Lucas Ponce
 */
public class TestScenarios {
    public static final String TEST_TENANT = "28026b36-8fe4-4332-84c8-524e173a68bf";

    public static String uuid() {
        return UUID.randomUUID().toString();
    }

    public static Collection<Event> marketingScenario() {
        // User1 buys 5 times in < 10 seconds
        long now = System.currentTimeMillis();
        Event e1 = new Event(TEST_TENANT, uuid(), now, "apm-data-id", "TraceCompletion", "Buy Book");
        e1.addTag("duration", "1000");
        e1.addTag("accountId", "user1");
        Event e2 = new Event(TEST_TENANT, uuid(), now + 1000, "apm-data-id", "TraceCompletion", "Buy Music");
        e2.addTag("duration", "2000");
        e2.addTag("accountId", "user1");
        Event e3 = new Event(TEST_TENANT, uuid(), now + 2000, "apm-data-id", "TraceCompletion", "Buy Groceries");
        e3.addTag("duration", "1500");
        e3.addTag("accountId", "user1");
        Event e4 = new Event(TEST_TENANT, uuid(), now + 3000, "apm-data-id", "TraceCompletion", "Buy VideoGames");
        e4.addTag("duration", "3000");
        e4.addTag("accountId", "user1");
        Event e5 = new Event(TEST_TENANT, uuid(), now + 4000, "apm-data-id", "TraceCompletion", "Buy VideoGames");
        e5.addTag("duration", "3000");
        e5.addTag("accountId", "user1");

        // User2 buys 3 times > 10 seconds
        Event e6 = new Event(TEST_TENANT, uuid(), now, "apm-data-id", "TraceCompletion", "Buy Book");
        e6.addTag("duration", "1000");
        e6.addTag("accountId", "user2");
        Event e7 = new Event(TEST_TENANT, uuid(), now + 5000, "apm-data-id", "TraceCompletion", "Buy Music");
        e7.addTag("duration", "2000");
        e7.addTag("accountId", "user2");
        Event e8 = new Event(TEST_TENANT, uuid(), now + 10000, "apm-data-id", "TraceCompletion", "Buy Groceries");
        e8.addTag("duration", "1500");
        e8.addTag("accountId", "user2");

        return Arrays.asList(e1, e2, e3, e4, e5, e6, e7, e8);
    }

    public static Collection<Event> fraudScenario() {
        // User1 buys 5 times in < 10 seconds from different locations
        long now = System.currentTimeMillis();
        Event e1 = new Event(TEST_TENANT, uuid(), now, "apm-data-id", "TraceCompletion", "Buy Book");
        e1.addTag("duration", "1000");
        e1.addTag("accountId", "user1");
        e1.addTag("location", "ip1");
        Event e2 = new Event(TEST_TENANT, uuid(), now + 1000, "apm-data-id", "TraceCompletion", "Buy Music");
        e2.addTag("duration", "2000");
        e2.addTag("accountId", "user1");
        e2.addTag("location", "ip1");
        Event e3 = new Event(TEST_TENANT, uuid(), now + 2000, "apm-data-id", "TraceCompletion", "Buy Groceries");
        e3.addTag("duration", "1500");
        e3.addTag("accountId", "user1");
        e3.addTag("location", "ip1");
        Event e4 = new Event(TEST_TENANT, uuid(), now + 3000, "apm-data-id", "TraceCompletion", "Buy VideoGames");
        e4.addTag("duration", "3000");
        e4.addTag("accountId", "user1");
        e4.addTag("location", "ip2");
        Event e5 = new Event(TEST_TENANT, uuid(), now + 4000, "apm-data-id", "TraceCompletion", "Buy VideoGames");
        e5.addTag("duration", "3000");
        e5.addTag("accountId", "user1");
        e5.addTag("location", "ip1");

        // User2 buys 3 times > 10 seconds from single location
        Event e6 = new Event(TEST_TENANT, uuid(), now, "apm-data-id", "TraceCompletion", "Buy Book");
        e6.addTag("duration", "1000");
        e6.addTag("accountId", "user2");
        e6.addTag("location", "ip3");
        Event e7 = new Event(TEST_TENANT, uuid(), now + 15000, "apm-data-id", "TraceCompletion", "Buy Music");
        e7.addTag("duration", "2000");
        e7.addTag("accountId", "user2");
        e7.addTag("location", "ip4");
        Event e8 = new Event(TEST_TENANT, uuid(), now + 20000, "apm-data-id", "TraceCompletion", "Buy Groceries");
        e8.addTag("duration", "1500");
        e8.addTag("accountId", "user2");
        e8.addTag("location", "ip5");

        // User3 buys 5 times in < 10 seconds from single location
        Event e11 = new Event(TEST_TENANT, uuid(), now, "apm-data-id", "TraceCompletion", "Buy Book");
        e11.addTag("duration", "1000");
        e11.addTag("accountId", "user3");
        e11.addTag("location", "ip10");
        Event e12 = new Event(TEST_TENANT, uuid(), now + 1000, "apm-data-id", "TraceCompletion", "Buy Music");
        e12.addTag("duration", "2000");
        e12.addTag("accountId", "user3");
        e12.addTag("location", "ip10");
        Event e13 = new Event(TEST_TENANT, uuid(), now + 2000, "apm-data-id", "TraceCompletion", "Buy Groceries");
        e13.addTag("duration", "1500");
        e13.addTag("accountId", "user3");
        e13.addTag("location", "ip10");
        Event e14 = new Event(TEST_TENANT, uuid(), now + 3000, "apm-data-id", "TraceCompletion", "Buy VideoGames");
        e14.addTag("duration", "3000");
        e14.addTag("accountId", "user3");
        e14.addTag("location", "ip10");
        Event e15 = new Event(TEST_TENANT, uuid(), now + 4000, "apm-data-id", "TraceCompletion", "Buy VideoGames");
        e15.addTag("duration", "3000");
        e15.addTag("accountId", "user3");
        e15.addTag("location", "ip10");

        return Arrays.asList(e1, e2, e3, e4, e5, e6, e7, e8, e11, e12, e13, e14, e15);
    }

    public static Collection<Event> customerRetentionScenario() {
        long now = System.currentTimeMillis();
        Event e1 = new Event(TEST_TENANT, uuid(), now, "apm-data-id", "Credit Check", "Exceptionally Good");
        e1.addTag("duration", "1000");
        e1.addTag("traceId", "trace1");
        e1.addTag("accountId", "user1");
        Event e2 = new Event(TEST_TENANT, uuid(), now + 1, "apm-data-id", "Stock Check", "Out of Stock");
        e2.addTag("duration", "2000");
        e2.addTag("traceId", "trace1");
        e2.addTag("accountId", "user1");

        Event e3 = new Event(TEST_TENANT, uuid(), now + 2, "apm-data-id", "Credit Check", "Good");
        e3.addTag("duration", "1500");
        e3.addTag("traceId", "trace2");
        e3.addTag("accountId", "user1");
        Event e4 = new Event(TEST_TENANT, uuid(), now + 3, "apm-data-id", "Stock Check", "Out of Stock");
        e4.addTag("duration", "2000");
        e4.addTag("traceId", "trace2");
        e4.addTag("accountId", "user1");

        Event e5 = new Event(TEST_TENANT, uuid(), now + 4, "apm-data-id", "Credit Check", "Exceptionally Good");
        e5.addTag("duration", "1500");
        e5.addTag("traceId", "trace3");
        e5.addTag("accountId", "user1");
        Event e6 = new Event(TEST_TENANT, uuid(), now + 5, "apm-data-id", "Stock Check", "Available");
        e6.addTag("duration", "2000");
        e6.addTag("traceId", "trace3");
        e6.addTag("accountId", "user1");

        Event e11 = new Event(TEST_TENANT, uuid(), now, "apm-data-id", "Credit Check", "Exceptionally Good");
        e11.addTag("duration", "1000");
        e11.addTag("traceId", "trace4");
        e11.addTag("accountId", "user2");
        Event e12 = new Event(TEST_TENANT, uuid(), now + 1, "apm-data-id", "Stock Check", "Out of Stock");
        e12.addTag("duration", "2000");
        e12.addTag("traceId", "trace4");
        e12.addTag("accountId", "user2");

        Event e13 = new Event(TEST_TENANT, uuid(), now + 2, "apm-data-id", "Credit Check", "Good");
        e13.addTag("duration", "1500");
        e13.addTag("traceId", "trace5");
        e13.addTag("accountId", "user2");
        Event e14 = new Event(TEST_TENANT, uuid(), now + 3, "apm-data-id", "Stock Check", "Out of Stock");
        e14.addTag("duration", "2000");
        e14.addTag("traceId", "trace5");
        e14.addTag("accountId", "user2");

        Event e15 = new Event(TEST_TENANT, uuid(), now + 4, "apm-data-id", "Credit Check", "Exceptionally Good");
        e15.addTag("duration", "1500");
        e15.addTag("traceId", "trace6");
        e15.addTag("accountId", "user2");
        Event e16 = new Event(TEST_TENANT, uuid(), now + 5, "apm-data-id", "Stock Check", "Available");
        e16.addTag("duration", "2000");
        e16.addTag("traceId", "trace6");
        e16.addTag("accountId", "user2");

        return Arrays.asList(e1, e2, e3, e4, e5, e6, e11, e12, e13, e14, e15, e16);
    }
}
