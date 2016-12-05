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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.hawkular.alerts.api.model.condition.ExternalCondition;
import org.hawkular.alerts.api.model.trigger.Trigger;

/**
 * Represent a DSL expression coming from an ExternalCondition which is parsed into a DRL format understandable
 * by the CEP engine.
 *
 * @author Jay Shaughnessy
 * @author Lucas Ponce
 */
public class Expression {

    private static final String DRL_HEADER = "  import org.hawkular.alerts.api.model.event.Event; \n" +
            "  import org.hawkular.alerts.api.json.JsonUtil; \n" +
            "  import java.util.List; \n" +
            "  import java.util.UUID; \n" +
            "  global java.util.List results; \n";
    private static final String SEPARATOR_TOKEN = ":";
    private static final String COMMA_TOKEN = ",";
    private static final String EVENT_TOKEN = "event";
    private static final String GROUP_BY_TOKEN = "groupBy(";
    private static final String TAGS_TOKEN = "tags.";
    private static final String FILTER_TOKEN = "filter(";
    private static final String HAVING_TOKEN = "having(";
    private static final String CTIME_CONSTRAINT = "$ctime : ctime";
    private static final String FIRST_TIME_TOKEN = "firstTime ";
    private static final String FIRST_TIME_VARIABLE = "\\$firstTime ";
    private static final String FIRST_TIME_FUNCTION = "$firstTime : min( $ctime )";
    private static final String LAST_TIME_TOKEN = "lastTime ";
    private static final String LAST_TIME_VARIABLE = "\\$lastTime ";
    private static final String LAST_TIME_FUNCTION = "$lastTime : max( $ctime )";
    private static final String COUNT_TAGS_TOKEN = "count.tags.";
    private static final String COUNT_TOKEN = "count ";
    private static final String COUNT_VARIABLE = "\\$count ";
    private static final String COUNT_FUNCTION = "$count : count( $event )";

    private static final String EVENTS_FUNCTION = "$events : collectList( $event )";

    private static final String BLANK = "                ";

    private static final Pattern TAGS_SEARCH = Pattern.compile("tags\\.(\\w+)\\s");
    private static final int GROUP_INDEX = 1;

    private String name;
    private String alerterId;
    private String expression;
    private String tenantId;
    private String source;
    private String dataId;

    private String drlGroupByDeclare;
    private String drlGroupByObject;
    private String drlGroupByConstraint;
    private Set<String> drlEventConstraints = new HashSet<>();
    private Set<String> drlFunctions = new HashSet<>(Arrays.asList(EVENTS_FUNCTION));
    private Set<String> drlFunctionsConstraints = new HashSet<>();

    private String drl;

    public Expression(Trigger trigger, ExternalCondition condition) {
        if (trigger == null || condition == null) {
            throw new IllegalArgumentException("Trigger or Condition must be not null");
        }
        name = trigger.getName() + "-" + condition.getConditionId();
        alerterId = condition.getAlerterId();
        expression = condition.getExpression();
        tenantId = trigger.getTenantId();
        source = trigger.getSource();
        dataId = condition.getDataId();

        if (isEmpty(expression)) {
            throw new IllegalArgumentException("Expression must be not null");
        }
        String[] section = expression.split(SEPARATOR_TOKEN);
        if (section.length < 3 || section.length > 4) {
            throw new IllegalArgumentException("Wrong sections for expression [" + expression + "]");
        }
        if (!section[0].equals(EVENT_TOKEN)) {
            throw new IllegalArgumentException("Expression [" + expression + "] must start with 'event'");
        }
        if (!section[1].startsWith(GROUP_BY_TOKEN)) {
            throw new IllegalArgumentException("Expression [" + expression + "] must contain a 'groupBy()' section");
        }
        parseGroupBy(section[1]);

        if (section.length == 3 && !section[2].startsWith(HAVING_TOKEN)) {
            throw new IllegalArgumentException("Expression [" + expression + "] has not a 'having()' section");
        }
        if (section.length == 4 && !section[2].startsWith(FILTER_TOKEN) && !section[3].startsWith(HAVING_TOKEN)) {
            throw new IllegalArgumentException("Expression [" + expression + "] has not a 'filter()' and 'having()' " +
                    "sections");
        }
        if (section.length == 3) {
            parseHaving(section[2]);
        } else {
            parseFilter(section[2]);
            parseHaving(section[3]);
        }
        buildDrl(name);
    }

    private void parseGroupBy(String section) {
        int endSection = section.lastIndexOf(')');
        if (endSection == -1) {
            throw new IllegalArgumentException("Expression [" + section + " must contain a valid 'groupBy()'");
        }
        String innerSection = section.substring(GROUP_BY_TOKEN.length(), endSection).trim();
        boolean tags = false;
        if (innerSection.startsWith(TAGS_TOKEN)) {
            tags = true;
        }
        String field;
        if (tags) {
            field = innerSection.substring(TAGS_TOKEN.length());
        } else {
            field = innerSection;
        }
        String type = makeType(field);
        drlGroupByObject = type + " ( $" + field + " : " + field + " )";
        if (tags) {
            drlGroupByConstraint = " tags[ \"" + field + "\" ] == $" + field + " ";
        } else {
            drlGroupByConstraint = " " + field + " == $" + field + " ";
        }
        drlEventConstraints.add(drlGroupByConstraint);
        drlGroupByDeclare =  "  declare " + type + " " + field + " : String end \n" +
                             "  rule \"Extract " + field + "\" \n" +
                             "  when \n" +
                             "    Event ( tenantId == \"" + tenantId + "\", \n" +
                             "            dataSource == \"" + source + "\", \n" +
                             "            dataId == \"" + dataId + "\", \n" +
                             "            $" + field + " : ";
        if (tags) {
            drlGroupByDeclare += "tags[ \"" + field + "\" ] != null ) \n ";
        } else {
            drlGroupByDeclare += field + " != null ) \n ";
        }
        drlGroupByDeclare += "   not " + type + " ( " + field + " == $" + field + " ) \n " +
                             " then \n " +
                             "   insert ( new " + type + " ( $" + field + " ) ); \n " +
                             " end \n ";
    }

    private void parseFilter(String section) {
        int endSection = section.lastIndexOf(')');
        if (endSection == -1) {
            throw new IllegalArgumentException("Expression [" + section + " must contain a valid 'filter()'");
        }
        String innerSection = section.substring(FILTER_TOKEN.length(), endSection).trim();
        String[] filterConstraints = innerSection.split(COMMA_TOKEN);
        for (int i = 0; i < filterConstraints.length; i++) {
            if (filterConstraints[i].contains(TAGS_TOKEN)) {
                filterConstraints[i] = replaceTags(filterConstraints[i]);
            }
            drlEventConstraints.add(filterConstraints[i]);
        }
    }

    private void parseHaving(String section) {
        int endSection = section.lastIndexOf(')');
        if (endSection == -1) {
            throw new IllegalArgumentException("Expression [" + section + " must contain a valid 'having()'");
        }
        String innerSection = section.substring(HAVING_TOKEN.length(), endSection).trim();
        String[] havingConstraints = innerSection.split(COMMA_TOKEN);
        for (int i = 0; i < havingConstraints.length; i++) {
            if (havingConstraints[i].contains(FIRST_TIME_TOKEN)) {
                havingConstraints[i] = havingConstraints[i].replaceAll(FIRST_TIME_TOKEN, FIRST_TIME_VARIABLE);
                drlEventConstraints.add(CTIME_CONSTRAINT);
                drlFunctions.add(FIRST_TIME_FUNCTION);
            }
            if (havingConstraints[i].contains(LAST_TIME_TOKEN)) {
                havingConstraints[i] = havingConstraints[i].replaceAll(LAST_TIME_TOKEN, LAST_TIME_VARIABLE);
                drlEventConstraints.add(CTIME_CONSTRAINT);
                drlFunctions.add(LAST_TIME_FUNCTION);
            }
            if (havingConstraints[i].contains(COUNT_TAGS_TOKEN)) {
                havingConstraints[i] = processCountTags(havingConstraints[i]);
            }
            if (havingConstraints[i].contains(COUNT_TOKEN)) {
                havingConstraints[i] = havingConstraints[i].replaceAll(COUNT_TOKEN, COUNT_VARIABLE);
                drlFunctions.add(COUNT_FUNCTION);
            }
            drlFunctionsConstraints.add(havingConstraints[i].trim());
        }
    }

    private void buildDrl(String name) {
        drl = DRL_HEADER + drlGroupByDeclare + " \n " +
                " rule \"" + name + "\" \n " +
                " when \n " +
                "   " + drlGroupByObject + " \n " +
                "   accumulate( $event : Event( tenantId == \"" + tenantId + "\", \n" +
                "                                dataSource == \"" + source + "\", \n" +
                "                                dataId == \"" + dataId + "\", \n";
        Iterator<String> it = drlEventConstraints.iterator();
        while (it.hasNext()) {
            String eventConstraint = it.next();
            drl += BLANK + BLANK + eventConstraint;
            if (it.hasNext()) {
                drl += ", \n";
            }
        }
        drl += "); \n";
        it = drlFunctions.iterator();
        while (it.hasNext()) {
            drl += BLANK + it.next();
            if (it.hasNext()) {
                drl += ", \n";
            }
        }
        drl += "; \n";
        it = drlFunctionsConstraints.iterator();
        while (it.hasNext()) {
            drl += BLANK + it.next();
            if (it.hasNext()) {
                drl += ", \n";
            }
        }
        drl += ") \n";
        drl +=  " then \n" +
                "   Event result = new Event(\"" + tenantId + "\", \n" +
                "                            UUID.randomUUID().toString(), \n" +
                "                            \"" + dataId +"\", \n" +
                "                            \"" + alerterId + "\", \n" +
                "                            \"" + expression.replaceAll("\"", "'") + "\"); \n" +
                "   result.addContext(\"events\", JsonUtil.toJson($events)); \n" +
                "   results.add( result ); \n" +
                "   $events.stream().forEach(e -> retract( e )); \n" +
                " end \n ";
    }

    public String getDrl() {
        return drl;
    }

    private String processCountTags(String str) {
        int start = str.indexOf(COUNT_TAGS_TOKEN);
        int end = str.indexOf(' ', start);
        String countTags = str.substring(start, end);
        String field = countTags.substring(COUNT_TAGS_TOKEN.length());
        drlFunctions.add("$" + field + "Set : collectSet($event.getTags().get(\"" + field + "\") )");
        return str.replaceAll(countTags, "\\$" + field + "Set.size");
    }

    private static String makeType(String field) {
        return field.substring(0, 1).toUpperCase() + field.substring(1);
    }

    private static String replaceTags(String str) {
        String newStr = str;
        Matcher matcher = TAGS_SEARCH.matcher(str);
        int index = 0;
        while (matcher.find(index)) {
            int end = matcher.end();
            String original = matcher.group();
            String field = matcher.group(GROUP_INDEX);
            index = end;
            newStr = newStr.replaceAll(original, "tags[\"" + field + "\"]");
        }
        return newStr;
    }

    private static boolean isEmpty(String s) {
        return null == s || s.trim().isEmpty();
    }
}
