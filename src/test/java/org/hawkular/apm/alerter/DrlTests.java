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

    private void runDrlWithEvents(String drl, Collection<Event> events, String...expected) {
        final KieBaseConfiguration kieBaseConfiguration = KnowledgeBaseFactory.newKnowledgeBaseConfiguration();
        final KieBase kieBase = new KieHelper().addContent(drl, ResourceType.DRL).build(kieBaseConfiguration);
        final KieSession kieSession = kieBase.newKieSession();
        kieSession.addEventListener(new DebugAgendaEventListener());
        kieSession.addEventListener(new DebugRuleRuntimeEventListener());

        List<String> results = new ArrayList<>();
        kieSession.setGlobal("results", results);

        final ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.submit(() -> kieSession.fireUntilHalt());

        events.stream().forEach(e -> kieSession.insert(e));

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kieSession.halt();
        kieSession.dispose();

        Assert.assertEquals(expected.length, results.size());
        Assert.assertTrue(results.containsAll(Arrays.asList(expected)));

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

    @Test
    public void marketingScenarioTest() {
        String drl = " import org.hawkular.alerts.api.model.event.Event; \n " +
                " import java.util.List; \n " +
                " global java.util.List results; \n" +
                " declare AccountId accountId : String end \n " +
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
                "   System.out.println(\"Account: \" + $accountId + \" has \" + $sizeEvents + " +
                "                      \" events in \" + ($lastEvent.longValue() - $firstEvent.longValue()) + \" ms \"); \n " +
                "   System.out.println(\"Events: \" + $eventList); \n " +
                "   $eventList.stream().forEach(e -> retract( e )); \n " +
                "   results.add( $accountId ); \n " +
                " end \n " +
                " \n ";

        runDrlWithEvents(drl, TestScenarios.marketingScenario(), "user1");
    }

    @Test
    public void fraudScenarioTest() {
        String drl = " import org.hawkular.alerts.api.model.event.Event; \n " +
                " import java.util.List; \n " +
                " global java.util.List results; \n" +
                " declare AccountId accountId : String end \n " +
                " rule \"Extract accountId\" \n " +
                " when \n " +
                "   Event( $accountId : tags[\"accountId\"] != null ) \n " +
                "   not AccountId ( accountId == $accountId  ) \n " +
                " then \n" +
                "   insert( new AccountId ( $accountId ) ); \n " +
                " end \n " +
                " \n " +
                " rule \"Fraud Scenario\" \n " +
                " when" +
                "   AccountId ( $accountId : accountId ) \n " +
                "   accumulate( $event : Event( tags[\"accountId\"] == $accountId, $ctime : ctime, $location : tags[\"location\"] ); \n " +
                "               $sizeEvents : count( $event ), \n" +
                "               $firstEvent : min( $ctime ), \n" +
                "               $lastEvent  : max( $ctime )," +
                "               $events : collectList( $event )," +
                "               $locations : collectSet( $location ); \n" +
                "               $sizeEvents > 1, \n " +
                "               $lastEvent - $firstEvent < 10000," +
                "               $locations.size > 1 ) \n " +
                " then \n " +
                "   System.out.println(\"Account: \" + $accountId + \" has \" + $sizeEvents + " +
                "                      \" events in \" + ($lastEvent.longValue() - $firstEvent.longValue()) " +
                "                      + \" ms from these locations: \" + $locations); \n " +
                "   System.out.println(\"Events: \" + $events); \n " +
                "   $events.stream().forEach(e -> retract( e )); \n " +
                "   results.add( $accountId ); \n " +
                " end \n " +
                " \n ";

        runDrlWithEvents(drl, TestScenarios.fraudScenario(), "user1");
    }

    @Test
    public void customerRetentionScenarioTest() {
        String drl = " import org.hawkular.alerts.api.model.event.Event; \n " +
                " import java.util.List; \n " +
                " global java.util.List results; \n" +
                " declare TraceId traceId : String end \n " +
                " rule \"Extract traceId\" \n " +
                " when \n " +
                "   Event( $traceId : tags[\"traceId\"] != null ) \n " +
                "   not TraceId ( traceId == $traceId  ) \n " +
                " then \n" +
                "   insert( new TraceId ( $traceId ) ); \n " +
                " end \n " +
                " \n " +
                " rule \"Custmer Retention Scenario\" \n " +
                " when" +
                "   TraceId ( $traceId : traceId ) \n " +
                "   accumulate( $event : Event( tags[\"traceId\"] == $traceId, \n" +
                "                               (category == \"Credit Check\" && text == \"Exceptionally Good\") || " +
                "                               (category == \"Stock Check\" && text == \"Out of Stock\") ); \n " +
                "               $sizeEvents : count( $event ), \n" +
                "               $events : collectList( $event )," +
                "               $users : collectSet( $event.getTags().get(\"accountId\") );" +
                "               $sizeEvents > 1," +
                "               $users.size == 1 ) \n " +
                " then \n " +
                "   System.out.println(\"TraceId: \" + $traceId + " +
                "                      \" for user: \" + $users + \" deserves a special offer. \"); " +
                "   System.out.println(\"Events: \" + $events); \n " +
                "   $events.stream().forEach(e -> retract( e )); \n " +
                "   results.addAll( $users ); \n " +
                " end \n " +
                " \n ";

        runDrlWithEvents(drl, TestScenarios.customerRetentionScenario(), "user1", "user2");
    }

}
