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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.drools.core.event.DebugAgendaEventListener;
import org.drools.core.event.DebugRuleRuntimeEventListener;
import org.hawkular.alerts.api.model.event.Event;
import org.junit.Assert;
import org.junit.Test;
import org.kie.api.KieBase;
import org.kie.api.KieBaseConfiguration;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.internal.KnowledgeBaseFactory;
import org.kie.internal.utils.KieHelper;

/**
 * @author Jay Shaughnessy
 * @author Lucas Ponce
 */
public class DrlTests {

    static String testTenant = "28026b36-8fe4-4332-84c8-524e173a68bf";

    String uuid() {
        return UUID.randomUUID().toString();
    }

    Collection<Event> marketingScenario() {
        // User1 buys 5 times in < 10 seconds
        long now = System.currentTimeMillis();
        Event e1 = new Event(testTenant, uuid(), now, "apm-data-id", "TraceCompletion", "Buy Book");
        e1.addTag("duration", "1000");
        e1.addTag("accountId", "user1");
        Event e2 = new Event(testTenant, uuid(), now + 1000, "apm-data-id", "TraceCompletion", "Buy Music");
        e2.addTag("duration", "2000");
        e2.addTag("accountId", "user1");
        Event e3 = new Event(testTenant, uuid(), now + 2000, "apm-data-id", "TraceCompletion", "Buy Groceries");
        e3.addTag("duration", "1500");
        e3.addTag("accountId", "user1");
        Event e4 = new Event(testTenant, uuid(), now + 3000, "apm-data-id", "TraceCompletion", "Buy VideoGames");
        e4.addTag("duration", "3000");
        e4.addTag("accountId", "user1");
        Event e5 = new Event(testTenant, uuid(), now + 4000, "apm-data-id", "TraceCompletion", "Buy VideoGames");
        e5.addTag("duration", "3000");
        e5.addTag("accountId", "user1");

        // User2 buys 3 times > 10 seconds
        Event e6 = new Event(testTenant, uuid(), now, "apm-data-id", "TraceCompletion", "Buy Book");
        e6.addTag("duration", "1000");
        e6.addTag("accountId", "user2");
        Event e7 = new Event(testTenant, uuid(), now + 5000, "apm-data-id", "TraceCompletion", "Buy Music");
        e7.addTag("duration", "2000");
        e7.addTag("accountId", "user2");
        Event e8 = new Event(testTenant, uuid(), now + 10000, "apm-data-id", "TraceCompletion", "Buy Groceries");
        e8.addTag("duration", "1500");
        e8.addTag("accountId", "user2");

        return Arrays.asList(e1, e2, e3, e4, e5, e6, e7, e8);
    }

    @Test
    public void marketingScenarioTest() {
        String drl = " import org.hawkular.alerts.api.model.event.Event; \n " +
                " import java.util.List; \n " +
                " declare AccountId accountId : String end \n " +
                " global java.util.List results; \n" +
                " rule \"Extract accountId\" \n " +
                " when \n " +
                "   Event( $accountId : tags[\"accountId\"] != null ) \n " +
                "   not AccountId ( accountId == $accountId  ) \n " +
                " then \n" +
                "   insert( new AccountId ( $accountId ) ); \n " +
                " end \n " +
                " \n " +
                " rule \"Marketing Scenario\" \n " +
                " when" +
                "   AccountId ( $accountId : accountId ) \n " +
                "   accumulate( $event : Event( tags[\"accountId\"] == $accountId, $ctime : ctime ); \n " +
                "               $sizeEvents : count( $event ), \n" +
                "               $firstEvent : min( $ctime ), \n" +
                "               $lastEvent  : max( $ctime )," +
                "               $eventList : collectList( $event ); \n" +
                "               $sizeEvents > 2, \n " +
                "               $lastEvent - $firstEvent < 10000 ) \n " +
                " then \n " +
                "   System.out.println(\"Account \" + $accountId + \" has \" + $sizeEvents + " +
                "                      \" events in \" + ($lastEvent.longValue() - $firstEvent.longValue()) + \" ms \"); \n " +
                "   System.out.println(\"Events: \" + $eventList); \n " +
                "   $eventList.stream().forEach(e -> retract( e )); \n " +
                "   results.add( $accountId ); \n " +
                " end \n " +
                " \n ";
        final KieBaseConfiguration kieBaseConfiguration = KnowledgeBaseFactory.newKnowledgeBaseConfiguration();
        final KieBase kieBase = new KieHelper().addContent(drl, ResourceType.DRL).build(kieBaseConfiguration);
        final KieSession kieSession = kieBase.newKieSession();
        kieSession.addEventListener(new DebugAgendaEventListener());
        kieSession.addEventListener(new DebugRuleRuntimeEventListener());

        List<String> results = new ArrayList<>();
        kieSession.setGlobal("results", results);

        final ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.submit(() -> kieSession.fireUntilHalt());

        marketingScenario().stream().forEach(e -> kieSession.insert(e));

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kieSession.halt();
        kieSession.dispose();

        Assert.assertEquals(1, results.size());
        Assert.assertTrue(results.contains("user1"));

        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }



}
