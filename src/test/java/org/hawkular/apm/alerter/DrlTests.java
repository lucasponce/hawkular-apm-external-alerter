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
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.drools.core.event.DebugAgendaEventListener;
import org.drools.core.event.DebugRuleRuntimeEventListener;
import org.drools.core.time.impl.PseudoClockScheduler;
import org.hawkular.alerts.api.model.event.Event;
import org.junit.Assert;
import org.junit.Test;
import org.kie.api.KieBase;
import org.kie.api.KieBaseConfiguration;
import org.kie.api.conf.EventProcessingOption;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.KieSessionConfiguration;
import org.kie.api.runtime.conf.ClockTypeOption;
import org.kie.internal.KnowledgeBaseFactory;
import org.kie.internal.utils.KieHelper;

/**
 * @author Jay Shaughnessy
 * @author Lucas Ponce
 */
public class DrlTests {

    public static final String TEST_TENANT = "28026b36-8fe4-4332-84c8-524e173a68bf";
    public static final String DATA_ID = "apm-data-id";
    public static final String ALERTER_ID = "HawkularAPM";

    KieBaseConfiguration kieBaseConfiguration;
    KieBase kieBase;
    KieSessionConfiguration kieSessionConf;
    KieSession kieSession;
    ExecutorService executor;
    List<String> results;
    PseudoClockScheduler clock;

    public static String uuid() {
        return UUID.randomUUID().toString();
    }

    private void startSession(String drl) {
        kieBaseConfiguration = KnowledgeBaseFactory.newKnowledgeBaseConfiguration();
        kieBaseConfiguration.setOption( EventProcessingOption.STREAM );
        kieBase = new KieHelper().addContent(drl, ResourceType.DRL).build(kieBaseConfiguration);
        kieSessionConf = KnowledgeBaseFactory.newKnowledgeSessionConfiguration();
        kieSessionConf.setOption( ClockTypeOption.get( "pseudo" ) );
        kieSession = kieBase.newKieSession(kieSessionConf, null);
        clock = kieSession.getSessionClock();
        kieSession.addEventListener(new DebugAgendaEventListener());
        kieSession.addEventListener(new DebugRuleRuntimeEventListener());
        results = new ArrayList<>();
        kieSession.setGlobal("results", results);
        executor = Executors.newFixedThreadPool(2);
        executor.submit(() -> kieSession.fireUntilHalt());
    }

    private void stopSession() {
        try {
            Thread.sleep(250);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kieSession.halt();
        kieSession.dispose();

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

    private void runDrlWithEvents(String drl, Collection<Event> events, List results) {
        final KieBaseConfiguration kieBaseConfiguration = KnowledgeBaseFactory.newKnowledgeBaseConfiguration();
        kieBaseConfiguration.setOption( EventProcessingOption.STREAM );
        final KieBase kieBase = new KieHelper().addContent(drl, ResourceType.DRL).build(kieBaseConfiguration);
        final KieSessionConfiguration sconf = KnowledgeBaseFactory.newKnowledgeSessionConfiguration();
        sconf.setOption( ClockTypeOption.get( "pseudo" ) );
        final KieSession kieSession = kieBase.newKieSession(sconf, null);
        kieSession.addEventListener(new DebugAgendaEventListener());
        kieSession.addEventListener(new DebugRuleRuntimeEventListener());

        kieSession.setGlobal("results", results);

        final ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.submit(() -> kieSession.fireUntilHalt());

        events.stream().forEach(e -> kieSession.insert(e));

        try {
            Thread.sleep(250);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kieSession.halt();
        kieSession.dispose();

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
                " declare Event \n" +
                "   @role( event ) \n" +
                "   @expires( 10m ) \n" +
                "   @timestamp( ctime ) \n" +
                " end \n\n" +
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
                "   accumulate( $event : Event( tags[\"accountId\"] == $accountId) over window:time(10s); \n " +
                "               $sizeEvents : count( $event ), \n" +
                "               $eventList : collectList( $event ); \n" +
                "               $sizeEvents > 2) \n " +
                " then \n " +
                "   System.out.println(\"Account: \" + $accountId + \" has \" + $sizeEvents + " +
                "                      \" events in 10s \"); \n " +
                "   System.out.println(\"Events: \" + $eventList); \n " +
                "   results.add( $accountId ); \n " +
                " end \n " +
                " \n ";

        startSession(drl);

        // Init t0
        long now = clock.getCurrentTime();

        // User1 buys 5 times in < 10 seconds
        Event e1 = new Event(TEST_TENANT, uuid(), now, DATA_ID, "TraceCompletion", "Buy Book");
        e1.addTag("duration", "1000");
        e1.addTag("accountId", "user1");

        // User2 buys 3 times > 10 seconds
        Event e6 = new Event(TEST_TENANT, uuid(), now, DATA_ID, "TraceCompletion", "Buy Book");
        e6.addTag("duration", "1000");
        e6.addTag("accountId", "user2");

        kieSession.insert(e1);
        kieSession.insert(e6);

        // t0 + 1000
        now = clock.advanceTime(1000, TimeUnit.MILLISECONDS);

        Event e2 = new Event(TEST_TENANT, uuid(), now, DATA_ID, "TraceCompletion", "Buy Music");
        e2.addTag("duration", "2000");
        e2.addTag("accountId", "user1");

        kieSession.insert(e2);

        // t0 + 2000
        now = clock.advanceTime(1000, TimeUnit.MILLISECONDS);
        Event e3 = new Event(TEST_TENANT, uuid(), now, DATA_ID, "TraceCompletion", "Buy Groceries");
        e3.addTag("duration", "1500");
        e3.addTag("accountId", "user1");

        kieSession.insert(e3);

        // t0 + 3000
        now = clock.advanceTime(1000, TimeUnit.MILLISECONDS);
        Event e4 = new Event(TEST_TENANT, uuid(), now, DATA_ID, "TraceCompletion", "Buy VideoGames");
        e4.addTag("duration", "3000");
        e4.addTag("accountId", "user1");

        kieSession.insert(e4);

        // t0 + 4000
        now = clock.advanceTime(1000, TimeUnit.MILLISECONDS);

        Event e5 = new Event(TEST_TENANT, uuid(), now, DATA_ID, "TraceCompletion", "Buy VideoGames");
        e5.addTag("duration", "3000");
        e5.addTag("accountId", "user1");

        kieSession.insert(e5);

        // t0 + 5000
        now = clock.advanceTime(1000, TimeUnit.MILLISECONDS);

        Event e7 = new Event(TEST_TENANT, uuid(), now, DATA_ID, "TraceCompletion", "Buy Music");
        e7.addTag("duration", "2000");
        e7.addTag("accountId", "user2");

        kieSession.insert(e7);

        // t0 + 10000
        now = clock.advanceTime(6000, TimeUnit.MILLISECONDS);

        Event e8 = new Event(TEST_TENANT, uuid(), now, DATA_ID, "TraceCompletion", "Buy Groceries");
        e8.addTag("duration", "1500");
        e8.addTag("accountId", "user2");

        kieSession.insert(e8);

        stopSession();

        Assert.assertEquals(1, results.size());
        Assert.assertTrue(results.contains("user1"));
    }

    @Test
    public void marketingScenarioGenerated() {
        String drl = "  import org.hawkular.alerts.api.model.event.Event; \n" +
                "  import org.hawkular.alerts.api.json.JsonUtil; \n" +
                "  import java.util.List; \n" +
                "  import java.util.UUID; \n" +
                "  global java.util.List results; \n" +
                "  declare AccountId accountId : String end \n" +
                "  rule \"Extract accountId\" \n" +
                "  when \n" +
                "    Event ( tenantId == \"28026b36-8fe4-4332-84c8-524e173a68bf\", \n" +
                "            dataSource == \"_none_\", \n" +
                "            dataId == \"apm-data-id\", \n" +
                "            $accountId : tags[ \"accountId\" ] != null ) \n" +
                "    not AccountId ( accountId == $accountId ) \n" +
                "  then \n" +
                "    insert ( new AccountId ( $accountId ) ); \n" +
                "  end \n" +
                "  \n" +
                "  rule \"Marketing Scenario--marketing-scenario-FIRING-1-1\" \n" +
                "  when \n" +
                "    AccountId ( $accountId : accountId ) \n" +
                "    accumulate( $event : Event( tenantId == \"28026b36-8fe4-4332-84c8-524e173a68bf\", \n" +
                "                                dataSource == \"_none_\", \n" +
                "                                dataId == \"apm-data-id\", \n" +
                "                                $ctime : ctime, \n" +
                "                                 tags[ \"accountId\" ] == $accountId ); \n" +
                "                $firstTime : min( $ctime ), \n" +
                "                $count : count( $event ), \n" +
                "                $events : collectList( $event ), \n" +
                "                $lastTime : max( $ctime ); \n" +
                "                $count > 2, \n" +
                "                $lastTime - $firstTime < 10000) \n" +
                " then \n" +
                "   Event result = new Event(\"28026b36-8fe4-4332-84c8-524e173a68bf\", \n" +
                "                            UUID.randomUUID().toString(), \n" +
                "                            \"apm-data-id\", \n" +
                "                            \"HawkularAPM\", \n" +
                "                            \"event:groupBy(tags.accountId):having(firstTime - lastTime < 10000, count > 2)\"); \n" +
                "   result.addContext(\"events\", JsonUtil.toJson($events)); \n" +
                "   results.add( result ); \n" +
                "   $events.stream().forEach(e -> retract( e )); \n" +
                " end \n";

        ArrayList<Event> results = new ArrayList<>();
        runDrlWithEvents(drl, TestScenarios.marketingScenario(), results);
        Assert.assertEquals(1, results.size());
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

        ArrayList<String> results = new ArrayList<>();
        runDrlWithEvents(drl, TestScenarios.fraudScenario(), results);
        Assert.assertEquals(1, results.size());
        Assert.assertTrue(results.contains("user1"));
    }

    @Test
    public void fraudScenarioGenerated() {
        String drl = "  import org.hawkular.alerts.api.model.event.Event; \n" +
                "  import org.hawkular.alerts.api.json.JsonUtil; \n" +
                "  import java.util.List; \n" +
                "  import java.util.UUID; \n" +
                "  global java.util.List results; \n" +
                "  declare AccountId accountId : String end \n" +
                "  rule \"Extract accountId\" \n" +
                "  when \n" +
                "    Event ( tenantId == \"28026b36-8fe4-4332-84c8-524e173a68bf\", \n" +
                "            dataSource == \"_none_\", \n" +
                "            dataId == \"apm-data-id\", \n" +
                "            $accountId : tags[ \"accountId\" ] != null ) \n" +
                "    not AccountId ( accountId == $accountId ) \n" +
                "  then \n" +
                "    insert ( new AccountId ( $accountId ) ); \n" +
                "  end \n" +
                "  \n" +
                "  rule \"Fraud Scenario--fraud-scenario-FIRING-1-1\" \n" +
                "  when \n" +
                "    AccountId ( $accountId : accountId ) \n" +
                "    accumulate( $event : Event( tenantId == \"28026b36-8fe4-4332-84c8-524e173a68bf\", \n" +
                "                                dataSource == \"_none_\", \n" +
                "                                dataId == \"apm-data-id\", \n" +
                "                                $ctime : ctime, \n" +
                "                                 tags[ \"accountId\" ] == $accountId ); \n" +
                "                $firstTime : min( $ctime ), \n" +
                "                $count : count( $event ), \n" +
                "                $events : collectList( $event ), \n" +
                "                $lastTime : max( $ctime ), \n" +
                "                $locationSet : collectSet($event.getTags().get(\"location\") ); \n" +
                "                $count > 1, \n" +
                "                $lastTime - $firstTime < 10000, \n" +
                "                $locationSet.size > 1) \n" +
                " then \n" +
                "   Event result = new Event(\"28026b36-8fe4-4332-84c8-524e173a68bf\", \n" +
                "                            UUID.randomUUID().toString(), \n" +
                "                            \"apm-data-id\", \n" +
                "                            \"HawkularAPM\", \n" +
                "                            \"event:groupBy(tags.accountId):having(lastTime - firstTime < 10000, count > 1, count.tags.location > 1)\"); \n" +
                "   result.addContext(\"events\", JsonUtil.toJson($events)); \n" +
                "   results.add( result ); \n" +
                "   $events.stream().forEach(e -> retract( e )); \n" +
                " end \n";
        ArrayList<Event> results = new ArrayList<>();
        runDrlWithEvents(drl, TestScenarios.fraudScenario(), results);
        Assert.assertEquals(1, results.size());
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

        ArrayList<String> results = new ArrayList<>();
        runDrlWithEvents(drl, TestScenarios.customerRetentionScenario(), results);
        Assert.assertEquals(2, results.size());
        Assert.assertTrue(results.contains("user1"));
        Assert.assertTrue(results.contains("user2"));
    }

    @Test
    public void customerRetentionScenarioGenerated() {
        String drl = "  import org.hawkular.alerts.api.model.event.Event; \n" +
                "  import org.hawkular.alerts.api.json.JsonUtil; \n" +
                "  import java.util.List; \n" +
                "  import java.util.UUID; \n" +
                "  global java.util.List results; \n" +
                "  declare TraceId traceId : String end \n" +
                "  rule \"Extract traceId\" \n" +
                "  when \n" +
                "    Event ( tenantId == \"28026b36-8fe4-4332-84c8-524e173a68bf\", \n" +
                "            dataSource == \"_none_\", \n" +
                "            dataId == \"apm-data-id\", \n" +
                "            $traceId : tags[ \"traceId\" ] != null ) \n" +
                "    not TraceId ( traceId == $traceId ) \n" +
                "  then \n" +
                "    insert ( new TraceId ( $traceId ) ); \n" +
                "  end \n" +
                "  \n" +
                "  rule \"Customer Retention Scenario--customer-retention-scenario-FIRING-1-1\" \n" +
                "  when \n" +
                "    TraceId ( $traceId : traceId ) \n" +
                "    accumulate( $event : Event( tenantId == \"28026b36-8fe4-4332-84c8-524e173a68bf\", \n" +
                "                                dataSource == \"_none_\", \n" +
                "                                dataId == \"apm-data-id\", \n" +
                "                                (category == \"Credit Check\" && text == \"Exceptionally Good\") ||(category == \"Stock Check\" && text == \"Out of Stock\"), \n" +
                "                                 tags[ \"traceId\" ] == $traceId ); \n" +
                "                $count : count( $event ), \n" +
                "                $events : collectList( $event ), \n" +
                "                $accountIdSet : collectSet($event.getTags().get(\"accountId\") ); \n" +
                "                $count > 1, \n" +
                "                $accountIdSet.size == 1) \n" +
                " then \n" +
                "   Event result = new Event(\"28026b36-8fe4-4332-84c8-524e173a68bf\", \n" +
                "                            UUID.randomUUID().toString(), \n" +
                "                            \"apm-data-id\", \n" +
                "                            \"HawkularAPM\", \n" +
                "                            \"event:groupBy(tags.traceId):filter((category == 'Credit Check' && text == 'Exceptionally Good') ||(category == 'Stock Check' && text == 'Out of Stock')):having(count > 1, count.tags.accountId == 1)\"); \n" +
                "   result.addContext(\"events\", JsonUtil.toJson($events)); \n" +
                "   results.add( result ); \n" +
                "   $events.stream().forEach(e -> retract( e )); \n" +
                " end ";

        ArrayList<Event> results = new ArrayList<>();
        runDrlWithEvents(drl, TestScenarios.customerRetentionScenario(), results);
        Assert.assertEquals(2, results.size());
    }

}
