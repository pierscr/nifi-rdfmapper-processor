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
 *//*

package it.eng.effector.processors.ais;

import it.eng.effector.processors.ais.parser.MessageParser;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"ais"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class AISNmeaProcessor extends AbstractProcessor {

    public static final PropertyDescriptor TIME_PATTERN = new PropertyDescriptor
            .Builder().name("TIME_PATTERN")
            .displayName("Time Pattern")
            .description("How to decode a date: input such as '20200101000000291' with pattern 'yyyyMMddHHmmssSSS' corresponds to  '2020-01-01 00:00:00.291 UTC'")
            .required(true)
            .defaultValue("yyyyMMddHHmmssSSS")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Sucessfully mapped attribute names")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to map attribute names")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(TIME_PATTERN);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
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

    private String extractMessage(FlowFile flowFile, ProcessSession session) throws IOException {
        final byte[] messageContent = new byte[(int) flowFile.getSize()];
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream inputStream) throws IOException {
                StreamUtils.fillBuffer(inputStream, messageContent, true);
            }
        });
        return IOUtils.toString(messageContent, StandardCharsets.UTF_8.toString());
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        MessageParser messageParser = new MessageParser(context.getProperty(TIME_PATTERN).getValue());
        try{
            String flowFileContent = this.extractMessage(flowFile, session);
            String converted = messageParser.convert(flowFileContent);
            session.getProvenanceReporter().send(flowFile,converted);
            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(InputStream inputStream, OutputStream outputStream) throws IOException {
                    IOUtils.write( converted, outputStream,  StandardCharsets.UTF_8);
                }
            });
            session.transfer(flowFile, REL_SUCCESS);
        }catch (Exception e){
            //send file to failure output
            getLogger().error(e.getMessage());
            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(InputStream inputStream, OutputStream outputStream) throws IOException {
                    IOUtils.write( "{\"error\":\""+e.getMessage()+"\"}", outputStream,  StandardCharsets.UTF_8);
                }
            });
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
*/
