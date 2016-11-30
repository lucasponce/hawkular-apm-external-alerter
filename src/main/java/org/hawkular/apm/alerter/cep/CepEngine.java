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
package org.hawkular.apm.alerter.cep;

import java.util.Collection;

import javax.ejb.Singleton;
import javax.ejb.Startup;

import org.hawkular.alerts.api.model.condition.ExternalCondition;
import org.kie.api.KieBase;
import org.kie.api.KieBaseConfiguration;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.internal.KnowledgeBaseFactory;
import org.kie.internal.utils.KieHelper;

/**
 * It evaluates events externally and send data into alerting
 *
 * @author Jay Shaughnessy
 * @author Lucas Ponce
 */
@Singleton
@Startup
public class CepEngine {

    KieSession kieSession;

    public void updateConditions(Collection<ExternalCondition> conditions) {
        // Here conditions will build the drl rules.
    }

    private void buildSession(String drl) {
        final KieBaseConfiguration kieBaseConfiguration = KnowledgeBaseFactory.newKnowledgeBaseConfiguration();
        final KieBase kieBase = new KieHelper().addContent(drl, ResourceType.DRL).build(kieBaseConfiguration);
        kieSession = kieBase.newKieSession();
    }



}
