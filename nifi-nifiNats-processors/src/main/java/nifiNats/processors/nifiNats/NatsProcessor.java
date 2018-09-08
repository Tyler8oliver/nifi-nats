/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nifiNats.processors.nifiNats;

import io.nats.client.Connection;
import io.nats.client.Nats;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.nio.charset.Charset;

@Tags({"nats"})
@CapabilityDescription("NiFi Nats Processor")
@SeeAlso({})
//@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
//@WritesAttributes({@WritesAttribute(attribute="", description="")})
public abstract class NatsProcessor extends AbstractProcessor {

    public static final PropertyDescriptor BROKERS = new PropertyDescriptor
            .Builder().name("Known Broker")
            .displayName("Known Broker")
            .description("NATS broker connection uri in the format nats://<host>:<port>")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("The Character Set that should be used to encode the textual content of the NATS message")
            .required(true)
            .defaultValue("UTF-8")
            .allowableValues(Charset.availableCharsets().keySet().toArray(new String[0]))
            .build();

    public static final PropertyDescriptor QUEUE = new PropertyDescriptor.Builder().name("Queue")
            .displayName("Queue")
            .description("NATS queue")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder().name("Topic")
            .displayName("Topic")
            .description("NATS Topic")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_BUFFER_SIZE = new PropertyDescriptor.Builder().name("Max Buffer Size")
            .description("The maximum amount of data to buffer in memory before sending to NATS")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .expressionLanguageSupported(false)
            .defaultValue("1 MB")
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Success relationship, published to NATS")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("Failure relationship")
            .build();

    protected Connection connection = null;

    protected List<PropertyDescriptor> descriptors;

    protected Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(BROKERS);
        descriptors.add(CHARSET);
        descriptors.add(QUEUE);
        descriptors.add(TOPIC);
        descriptors.add(MAX_BUFFER_SIZE);

        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
       return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    protected Connection getConnection(final ProcessContext context) {
        return this.connection != null ? this.connection : this.createConnection(context);
    }

    protected Connection createConnection(final ProcessContext context) {
        try {
            Connection nc = Nats.connect(context.getProperty(BROKERS).getValue());
            this.connection = nc;
            return nc;
        } catch(IOException | InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    protected void closeConnection(Connection nc) {
        try {
            nc.close();
            this.connection = null;
        } catch (InterruptedException ex) {
            getLogger().warn("Failed to close NATS connection.", ex);
        }
    }
}
