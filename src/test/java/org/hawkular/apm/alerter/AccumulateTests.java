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
import org.kie.api.conf.EventProcessingOption;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.internal.KnowledgeBaseFactory;
import org.kie.internal.utils.KieHelper;

/**
 * @author Jay Shaughnessy
 * @author Lucas Ponce
 */
public class AccumulateTests {

    @Test
    public void incrementalAccumulate() {
        String drl = " import org.hawkular.alerts.api.model.event.Event; \n " +
                " import java.util.List; \n " +
                " \n " +
                "  declare Event \n" +
                "    @role( event ) \n" +
                "    @expires( 10m ) \n" +
                "    @timestamp( ctime ) \n" +
                "  end \n\n" +
                " rule \"Accumulate test\" \n " +
                " when" +
                "   accumulate( $event : Event() over window:time(10s); \n " +
                "               $sizeEvents : count( $event ), \n" +
                "               $eventList : collectList( $event ); \n" +
                "               $sizeEvents > 2 ) \n " +
                " then \n " +
                "   System.out.println(\"Events [\" + $eventList.size() + \"]: \" + $eventList); \n " +
                " end \n " +
                " \n ";

        final KieBaseConfiguration kieBaseConfiguration = KnowledgeBaseFactory.newKnowledgeBaseConfiguration();
        kieBaseConfiguration.setOption( EventProcessingOption.STREAM );
        final KieBase kieBase = new KieHelper().addContent(drl, ResourceType.DRL).build(kieBaseConfiguration);
        final KieSession kieSession = kieBase.newKieSession();
        kieSession.addEventListener(new DebugAgendaEventListener());
        kieSession.addEventListener(new DebugRuleRuntimeEventListener());

        final ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.submit(() -> kieSession.fireUntilHalt());

        Event e1 = new Event("tenant", "1", System.currentTimeMillis(), "dataId", "TraceCompletion", "Buy Book 1");
        Event e2 = new Event("tenant", "2", System.currentTimeMillis(), "dataId", "TraceCompletion", "Buy Book 2");
        Event e3 = new Event("tenant", "3", System.currentTimeMillis(), "dataId", "TraceCompletion", "Buy Book 3");

        kieSession.insert(e1);
        kieSession.insert(e2);
        kieSession.insert(e3);

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Event e4 = new Event("tenant", "4", System.currentTimeMillis(), "dataId", "TraceCompletion", "Buy Book 4");
        Event e5 = new Event("tenant", "5", System.currentTimeMillis(), "dataId", "TraceCompletion", "Buy Book 5");
        Event e6 = new Event("tenant", "6", System.currentTimeMillis(), "dataId", "TraceCompletion", "Buy Book 6");

        kieSession.insert(e4);
        kieSession.insert(e5);
        kieSession.insert(e6);

        try {
            Thread.sleep(500);
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
}
