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

import static org.hawkular.alerts.api.model.trigger.Mode.FIRING;
import static org.hawkular.apm.alerter.TestScenarios.ALERTER_ID;
import static org.hawkular.apm.alerter.TestScenarios.DATA_ID;
import static org.hawkular.apm.alerter.TestScenarios.TEST_TENANT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.drools.core.event.DebugAgendaEventListener;
import org.drools.core.event.DebugRuleRuntimeEventListener;
import org.hawkular.alerts.api.json.JsonUtil;
import org.hawkular.alerts.api.model.condition.ExternalCondition;
import org.hawkular.alerts.api.model.event.Event;
import org.hawkular.alerts.api.model.trigger.FullTrigger;
import org.hawkular.alerts.api.model.trigger.Trigger;
import org.junit.Assert;
import org.junit.Test;
import org.kie.api.KieBase;
import org.kie.api.KieBaseConfiguration;
import org.kie.api.conf.EventProcessingOption;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.internal.KnowledgeBaseFactory;
import org.kie.internal.utils.KieHelper;

/**
 * @author Jay Shaughnessy
 * @author Lucas Ponce
 */
public class ExpressionTest {

    private Collection<Event> runDrlWithEvents(String drl, Collection<Event> events) {
        final KieBaseConfiguration kieBaseConfiguration = KnowledgeBaseFactory.newKnowledgeBaseConfiguration();
        kieBaseConfiguration.setOption( EventProcessingOption.STREAM );
        final KieBase kieBase = new KieHelper().addContent(drl, ResourceType.DRL).build(kieBaseConfiguration);
        final KieSession kieSession = kieBase.newKieSession();
        kieSession.addEventListener(new DebugAgendaEventListener());
        kieSession.addEventListener(new DebugRuleRuntimeEventListener());

        List<Event> results = new ArrayList<>();
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

        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        } finally {
            return results;
        }
    }

    @Test
    public void marketingScenarioDsl() {
        Trigger trigger = new Trigger(TEST_TENANT, "marketing-scenario","Marketing Scenario");
        String expression = "event:groupBy(tags.accountId):having(lastTime - firstTime < 10000, count > 2)";
        ExternalCondition condition = new ExternalCondition(trigger.getId(), FIRING, DATA_ID, ALERTER_ID, expression);
        Expression exp = new Expression(Arrays.asList(new FullTrigger(trigger, null, Arrays.asList(condition))));
        Collection<Event> results = runDrlWithEvents(exp.getDrl(), TestScenarios.marketingScenario());
        Assert.assertEquals(1, results.size());
        Event result = results.iterator().next();
        Assert.assertEquals("user1", result.getContext().get("accountId"));
        List<Event> events = extractEvents(result);
        Assert.assertEquals(5, events.size());
    }

    @Test
    public void fraudScenarioDsl() {
        Trigger trigger = new Trigger(TEST_TENANT, "fraud-scenario", "Fraud Scenario");
        String expression = "event:groupBy(tags.accountId):having(lastTime - firstTime < 10000, " +
                "count > 1, count.tags.location > 1)";
        ExternalCondition condition = new ExternalCondition(trigger.getId(), FIRING, DATA_ID, ALERTER_ID, expression);
        Expression exp = new Expression(Arrays.asList(new FullTrigger(trigger, null, Arrays.asList(condition))));
        Collection<Event> results = runDrlWithEvents(exp.getDrl(), TestScenarios.fraudScenario());
        Assert.assertEquals(1, results.size());
        Event result = results.iterator().next();
        Assert.assertEquals("user1", result.getContext().get("accountId"));
        List<Event> events = extractEvents(result);
        Assert.assertEquals(5, events.size());
    }

    @Test
    public void customerRetentionScenarioDsl() {
        Trigger trigger = new Trigger(TEST_TENANT, "customer-retention-scenario", "Customer Retention Scenario");
        String expression = "event:groupBy(tags.traceId):" +
                "filter((category == \"Credit Check\" && text == \"Exceptionally Good\") || " +
                       "(category == \"Stock Check\" && text == \"Out of Stock\")):" +
                "having(count > 1, count.tags.accountId == 1)";
        ExternalCondition condition = new ExternalCondition(trigger.getId(), FIRING, DATA_ID, ALERTER_ID, expression);
        Expression exp = new Expression(Arrays.asList(new FullTrigger(trigger, null, Arrays.asList(condition))));
        Collection<Event> results = runDrlWithEvents(exp.getDrl(), TestScenarios.customerRetentionScenario());
        Assert.assertEquals(2, results.size());
        Event result = results.iterator().next();
        Assert.assertTrue(Arrays.asList("trace1", "trace4").contains(result.getContext().get("traceId")));
        List<Event> events = extractEvents(result);
        Assert.assertEquals(2, events.size());
        result = results.iterator().next();
        Assert.assertTrue(Arrays.asList("trace1", "trace4").contains(result.getContext().get("traceId")));
        events = extractEvents(result);
        Assert.assertEquals(2, events.size());
    }

    @Test
    public void combinedScenarios() {
        List<FullTrigger> activeTriggers = new ArrayList<>();

        // Marketing
        Trigger trigger = new Trigger(TEST_TENANT, "marketing-scenario","Marketing Scenario");
        String expression = "event:groupBy(tags.accountId):having(lastTime - firstTime < 10000, count > 2)";
        ExternalCondition condition = new ExternalCondition(trigger.getId(), FIRING, DATA_ID, ALERTER_ID, expression);
        activeTriggers.add(new FullTrigger(trigger, null, Arrays.asList(condition)));

        // Fraud
        trigger = new Trigger(TEST_TENANT, "fraud-scenario", "Fraud Scenario");
        expression = "event:groupBy(tags.accountId):having(lastTime - firstTime < 10000, " +
                "count > 1, count.tags.location > 1)";
        condition = new ExternalCondition(trigger.getId(), FIRING, DATA_ID, ALERTER_ID, expression);
        activeTriggers.add(new FullTrigger(trigger, null, Arrays.asList(condition)));

        // Customer retention
        trigger = new Trigger(TEST_TENANT, "customer-retention-scenario", "Customer Retention Scenario");
        expression = "event:groupBy(tags.traceId):" +
                "filter((category == \"Credit Check\" && text == \"Exceptionally Good\") || " +
                "(category == \"Stock Check\" && text == \"Out of Stock\")):" +
                "having(count > 1, count.tags.accountId == 1)";
        condition = new ExternalCondition(trigger.getId(), FIRING, DATA_ID, ALERTER_ID, expression);
        activeTriggers.add(new FullTrigger(trigger, null, Arrays.asList(condition)));

        Expression exp = new Expression(activeTriggers);
        Collection<Event> combinedScenarios = new ArrayList<>();
        combinedScenarios.addAll(TestScenarios.marketingScenario());
        combinedScenarios.addAll(TestScenarios.fraudScenario());
        combinedScenarios.addAll(TestScenarios.customerRetentionScenario());

        Collection<Event> results = runDrlWithEvents(exp.getDrl(), combinedScenarios);

        Assert.assertEquals(5, results.size());

    }

    public static List<Event> extractEvents(Event e) {
        List<Event> events = new ArrayList<>();
        JsonUtil.fromJson(e.getContext().get("events"), ArrayList.class).stream()
                .forEach(o -> {
                    events.add(JsonUtil.fromJson(JsonUtil.toJson(o), Event.class));
                });
        return events;
    }

}
