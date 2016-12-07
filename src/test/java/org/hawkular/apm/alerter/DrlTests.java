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

    private void runDrlWithEvents(String drl, Collection<Event> events, List results) {
        final KieBaseConfiguration kieBaseConfiguration = KnowledgeBaseFactory.newKnowledgeBaseConfiguration();
        // kieBaseConfiguration.setOption( EventProcessingOption.STREAM );
        final KieBase kieBase = new KieHelper().addContent(drl, ResourceType.DRL).build(kieBaseConfiguration);
        final KieSession kieSession = kieBase.newKieSession();
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

        ArrayList<String> results = new ArrayList<>();
        runDrlWithEvents(drl, TestScenarios.marketingScenario(), results);
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
