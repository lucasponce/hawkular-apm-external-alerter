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
import org.hawkular.alerts.api.json.JsonUtil;
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
public class ExpressionTest {

    private Collection<Event> runDrlWithEvents(String drl, Collection<Event> events) {
        final KieBaseConfiguration kieBaseConfiguration = KnowledgeBaseFactory.newKnowledgeBaseConfiguration();
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
        String expression = "event:groupBy(tags.accountId):having(firstTime - lastTime < 10000, count > 2)";

        Expression exp = new Expression("Marketing Scenario", expression);
        System.out.println("RULE: \n");
        System.out.println(exp.getDrl());
        Collection<Event> results = runDrlWithEvents(exp.getDrl(), TestScenarios.marketingScenario());
        JsonUtil.toJson(results);
        System.out.println(JsonUtil.toJson(results));
    }

    @Test
    public void fraudScenarioDsl() {
        String expression = "event:groupBy(tags.accountId):having(firstTime - lastTime < 10000, " +
                "count > 1, count.tags.location > 1)";

        Expression exp = new Expression("Fraud Scenario", expression);
        Assert.assertEquals("", exp.getDrl());
    }

    @Test
    public void customerRetentionScenarioDsl() {
        String expression = "event:groupBy(tags.traceId):" +
                "filter((category == \"Credit Check\" && text == \"Exceptionally Good\") ||" +
                       "(category == \"Stock Check\" && text == \"Out of Stock\")):" +
                "having(count > 1, count.tags.accountId == 1)";

        Expression exp = new Expression("Customer Retention Scenario", expression);
        Assert.assertEquals("", exp.getDrl());
    }
}
