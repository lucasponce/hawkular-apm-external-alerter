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

import org.hawkular.alerts.api.model.action.ActionDefinition
import org.hawkular.alerts.api.model.condition.ExternalCondition
import org.hawkular.alerts.api.model.event.Event
import org.hawkular.alerts.api.model.export.Definitions
import org.hawkular.alerts.api.model.trigger.FullTrigger
import org.hawkular.alerts.api.model.trigger.Trigger
import org.junit.Test

import static org.hawkular.alerts.api.model.trigger.Mode.FIRING
import static org.junit.Assert.assertEquals

/**
 * Events REST tests.
 *
 * @author Lucas Ponce
 */
class EventsITest extends AbstractITestBase {

    public static final String TEST_TENANT = "28026b36-8fe4-4332-84c8-524e173a68bf";
    public static final String DATA_ID = "apm-data-id";
    public static final String ALERTER_ID = "HawkularAPM";

    private static final String TAG_NAME = "HawkularAPM";
    private static final String TAG_VALUE = "CorrelationEvaluation";


    String uuid() {
        return UUID.randomUUID().toString()
    }

    void sleep(long milliseconds) {
        System.out.println("Sleeping [" + milliseconds + "] ms");
        Thread.sleep(milliseconds);
    }

    long now() {
        return System.currentTimeMillis();
    }

    @Test
    void marketingScenarioRealTime() {

        Trigger trigger = new Trigger(TEST_TENANT, "fraud-scenario", "Fraud Scenario")
        trigger.addTag(TAG_NAME, TAG_VALUE)
        trigger.setEnabled(true)
        String expression = "event:groupBy(context.accountId):window(time,10s):having(count > 2)"
        ExternalCondition condition = new ExternalCondition(trigger.getId(), FIRING, DATA_ID, ALERTER_ID, expression)
        condition.setAlerterId(TAG_NAME)

        Definitions definitions = new Definitions(
                Arrays.asList(new FullTrigger(trigger, null, Arrays.asList(condition))),
                new ArrayList<ActionDefinition>()
        )

        def resp = alerts.post(path: "import/delete", body: definitions)
        assertEquals(200, resp.status)

        // Let some time triggers to update the definitions

        sleep(3000)

        // t0

        // User1 buys 5 times in < 10 seconds
        Event e1 = new Event(TEST_TENANT, uuid(), now(), DATA_ID, "TraceCompletion", "E1 - Buy Book")
        e1.addContext("duration", "1000")
        e1.addContext("accountId", "user1")

        // User2 buys 3 times > 10 seconds
        Event e6 = new Event(TEST_TENANT, uuid(), now(), DATA_ID, "TraceCompletion", "E6 - Buy Book")
        e6.addContext("duration", "1000")
        e6.addContext("accountId", "user2")

        alerter.post(path: "events/data", body: Arrays.asList(e1, e6))
        assertEquals(200, resp.status)

        // t0 + 1000
        sleep(1000)

        Event e2 = new Event(TEST_TENANT, uuid(), now(), DATA_ID, "TraceCompletion", "E2 - Buy Music")
        e2.addContext("duration", "2000")
        e2.addContext("accountId", "user1")

        alerter.post(path: "events/data", body: Arrays.asList(e2))
        assertEquals(200, resp.status)


        // t0 + 2000
        sleep(1000)

        Event e3 = new Event(TEST_TENANT, uuid(), now(), DATA_ID, "TraceCompletion", "E3 - Buy Groceries")
        e3.addContext("duration", "1500")
        e3.addContext("accountId", "user1")

        alerter.post(path: "events/data", body: Arrays.asList(e3))
        assertEquals(200, resp.status)

        // t0 + 3000
        sleep(1000)

        Event e4 = new Event(TEST_TENANT, uuid(), now(), DATA_ID, "TraceCompletion", "E4 - Buy VideoGames")
        e4.addContext("duration", "3000")
        e4.addContext("accountId", "user1")

        alerter.post(path: "events/data", body: Arrays.asList(e4))
        assertEquals(200, resp.status)

        // t0 + 4000
        sleep(1000)

        Event e5 = new Event(TEST_TENANT, uuid(), now(), DATA_ID, "TraceCompletion", "E5 - Buy VideoGames")
        e5.addContext("duration", "3000")
        e5.addContext("accountId", "user1")

        alerter.post(path: "events/data", body: Arrays.asList(e5))
        assertEquals(200, resp.status)

        // t0 + 5000
        sleep(1000)

        Event e7 = new Event(TEST_TENANT, uuid(), now(), DATA_ID, "TraceCompletion", "E7 - Buy Music")
        e7.addContext("duration", "2000")
        e7.addContext("accountId", "user2")

        alerter.post(path: "events/data", body: Arrays.asList(e7))
        assertEquals(200, resp.status)

        // t0 + 11000
        sleep(6000)

        Event e8 = new Event(TEST_TENANT, uuid(), now(), DATA_ID, "TraceCompletion", "E8 - Buy Groceries")
        e8.addContext("duration", "1500")
        e8.addContext("accountId", "user2")

        alerter.post(path: "events/data", body: Arrays.asList(e8))
        assertEquals(200, resp.status)
    }

}
