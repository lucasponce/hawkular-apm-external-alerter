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

import org.hawkular.alerts.api.model.event.Event;
import org.hawkular.alerts.api.model.trigger.FullTrigger;

/**
 * It process result events from the CepEngineImpl
 *
 * @author Jay Shaughnessy
 * @author Lucas Ponce
 */
public interface CepEngine {

    void updateConditions(Collection<FullTrigger> activeTriggers);

    void processEvents(Collection<Event> events);

    void sendResult(Event event);
}
