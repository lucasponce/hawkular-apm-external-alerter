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
package org.hawkular.apm.alerter.rest;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import static org.hawkular.apm.alerter.rest.HawkularApmAlerterApp.TENANT_HEADER_NAME;

import java.util.Collection;

import javax.ws.rs.Consumes;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import org.hawkular.alerts.api.model.event.Event;
import org.jboss.logging.Logger;

/**
 * REST endpoint for events
 *
 * @author Jay Shaughnessy
 * @author Lucas Ponce
 */
@Path("/events")
public class EventsHandler {
    private final Logger log = Logger.getLogger(EventsHandler.class);

    @HeaderParam(TENANT_HEADER_NAME)
    String tenantId;

    public EventsHandler() {
        log.debug("Creating instance.");
    }

    @POST
    @Path("/data")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public Response sendEvents(final Collection<Event> events) {
        try {
            if (isEmpty(events)) {
                return ResponseUtil.badRequest("Events are empty");
            } else {
                events.stream().forEach(e -> e.setTenantId(tenantId));
                log.infof("Events received: %s", events);
                return ResponseUtil.ok();
            }
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            if (e.getCause() != null && e.getCause() instanceof IllegalArgumentException) {
                return ResponseUtil.badRequest("Bad arguments: " + e.getMessage());
            }
            return ResponseUtil.internalError(e);
        }
    }

    private boolean isEmpty(Collection collection) {
        return collection == null || collection.isEmpty();
    }

}
