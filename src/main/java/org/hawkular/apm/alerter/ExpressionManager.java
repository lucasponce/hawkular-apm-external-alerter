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

import static org.hawkular.alerts.api.services.DefinitionsEvent.Type.TRIGGER_REMOVE;
import static org.hawkular.alerts.api.services.DefinitionsEvent.Type.TRIGGER_UPDATE;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.inject.Inject;

import org.hawkular.alerts.api.model.condition.Condition;
import org.hawkular.alerts.api.model.condition.ExternalCondition;
import org.hawkular.alerts.api.model.trigger.Trigger;
import org.hawkular.alerts.api.services.DefinitionsListener;
import org.hawkular.alerts.api.services.DefinitionsService;
import org.hawkular.apm.alerter.cep.CepEngine;
import org.jboss.logging.Logger;

/**
 * Manages the APM expression evaluations and interacts with the Alerts system.
 *
 * @author Jay Shaughnessy
 * @author Lucas Ponce
 */
@Startup
@Singleton
public class ExpressionManager {
    private final Logger log = Logger.getLogger(ExpressionManager.class);

    private static final String TAG_NAME = "HawkularAPM";
    private static final String TAG_VALUE = "CorrelationEvaluation";

    private DefinitionsListener definitionsListener = null;

    @Inject
    private DefinitionsService definitions;

    @Inject
    private CepEngine cep;

    @PostConstruct
    public void init() {
        if (null == definitionsListener) {
            log.info("Registering Trigger UPDATE/REMOVE listener");
            definitions.registerListener(e -> refresh(), TRIGGER_UPDATE, TRIGGER_REMOVE);
        }
    }

    private void refresh() {
        try {
            Set<ExternalCondition> activeConditions = new HashSet<>();

            // get all of the triggers tagged for hawkular metrics
            Collection<Trigger> triggers = definitions.getAllTriggersByTag(TAG_NAME, TAG_VALUE);
            log.info("Found [" + triggers.size() + "] External Metrics Triggers!");

            Collection<Condition> conditions = null;
            for (Trigger trigger : triggers) {
                try {
                    if (trigger.isEnabled()) {
                        conditions = definitions.getTriggerConditions(trigger.getTenantId(), trigger.getId(), null);
                        log.info("Checking [" + conditions.size() + "] Conditions for enabled trigger ["
                                + trigger.getName() + "]!");
                    }
                } catch (Exception e) {
                    log.error("Failed to fetch Conditions when scheduling metrics conditions for " + trigger, e);
                    continue;
                }
                if (null == conditions) {
                    continue;
                }
                for (Condition condition : conditions) {
                    if (condition instanceof ExternalCondition) {
                        ExternalCondition externalCondition = (ExternalCondition) condition;
                        if (TAG_NAME.equals(externalCondition.getAlerterId())) {
                            activeConditions.add(externalCondition);
                        }
                    }
                }
            }
            log.infof("ActiveConditions: %s", activeConditions);
            cep.updateConditions(activeConditions);
        } catch (Exception e) {
            log.error("Failed to fetch Triggers for external conditions.", e);
        }
    }
}
