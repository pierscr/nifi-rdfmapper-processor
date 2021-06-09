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
package it.eng.effector.processors.ais;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import it.eng.effector.rmlmapper.*;

@Tags({"ais"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class RDFMapperProcessor extends AbstractProcessor {

    private static final Logger logger = LoggerFactory.getLogger(RDFMapperProcessor.class);

    public static final PropertyDescriptor RML_MAPPER_FILE = new PropertyDescriptor
            .Builder().name("RML_MAPPER_FILE")
            .displayName("RML mapper file path")
            .description("The path of the rml file that map json to RDF")
            .required(true)
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
        descriptors.add(RML_MAPPER_FILE);
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



    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        logger.debug("nifi flowfile on trigger"+flowFile.getId());
        final String rmlFileMapper=context.getProperty(RML_MAPPER_FILE).getValue();
        try {
            session.write(flowFile, new StreamCallback() {
                @Override
                public void process(final InputStream inputStream, OutputStream outputStream) throws IOException {
                    NifiFlowFileAccess nifiAccess = new NifiFlowFileAccess(inputStream);
                    NifiFlowFileWrite nifiFlowFileWrite= new NifiFlowFileWrite(outputStream);
                    RMLEngine engine=new RMLEngine(nifiAccess,nifiFlowFileWrite);
                    try {
                        engine.run(rmlFileMapper);
                    } catch (Exception e) {
                        throw new IOException(e);
                    }

                }
            });
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(InputStream inputStream, OutputStream outputStream) throws IOException {
                    IOUtils.write( "{'error':'"+e.getMessage()+"','stack':'"+ ExceptionUtils.getStackTrace(e)+"'}", outputStream,  StandardCharsets.UTF_8);
                }
            });
            logger.error(e.getStackTrace().toString());
            session.transfer(flowFile, REL_FAILURE);
        }


    }
}
