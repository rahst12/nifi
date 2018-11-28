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
package org.apache.nifi.processors.standard;

import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.*;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.jolt.TransformFactory;
import org.apache.nifi.processors.standard.util.jolt.TransformUtils;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;

import com.bazaarvoice.jolt.JoltTransform;
import com.bazaarvoice.jolt.JsonUtils;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"json", "jolt", "transform", "shiftr", "chainr", "defaultr", "removr","cardinality","sort"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttribute(attribute = "mime.type",description = "Always set to application/json")
@CapabilityDescription("Applies a list of Jolt specifications to the flowfile JSON payload. A new FlowFile is created "
        + "with transformed content and is routed to the 'success' relationship. If the JSON transform "
        + "fails, the original FlowFile is routed to the 'failure' relationship.")
public class JoltTransformJSON extends AbstractProcessor {

    public static final AllowableValue SHIFTR = new AllowableValue("jolt-transform-shift", "Shift", "Shift input JSON/data to create the output JSON.");
    public static final AllowableValue CHAINR = new AllowableValue("jolt-transform-chain", "Chain", "Execute list of Jolt transformations.");
    public static final AllowableValue DEFAULTR = new AllowableValue("jolt-transform-default", "Default", " Apply default values to the output JSON.");
    public static final AllowableValue REMOVR = new AllowableValue("jolt-transform-remove", "Remove", " Remove values from input data to create the output JSON.");
    public static final AllowableValue CARDINALITY = new AllowableValue("jolt-transform-card", "Cardinality", "Change the cardinality of input elements to create the output JSON.");
    public static final AllowableValue SORTR = new AllowableValue("jolt-transform-sort", "Sort", "Sort input json key values alphabetically. Any specification set is ignored.");
    public static final AllowableValue CUSTOMR = new AllowableValue("jolt-transform-custom", "Custom", "Custom Transformation. Requires Custom Transformation Class Name");
    public static final AllowableValue MODIFIER_DEFAULTR = new AllowableValue("jolt-transform-modify-default", "Modify - Default", "Writes when key is missing or value is null");
    public static final AllowableValue MODIFIER_OVERWRITER = new AllowableValue("jolt-transform-modify-overwrite", "Modify - Overwrite", " Always overwrite value");
    public static final AllowableValue MODIFIER_DEFINER = new AllowableValue("jolt-transform-modify-define", "Modify - Define", "Writes when key is missing");

    public static final PropertyDescriptor JOLT_TRANSFORM = new PropertyDescriptor.Builder()
            .name("jolt-transform")
            .displayName("Jolt Transformation DSL")
            .description("Specifies the Jolt Transformation that should be used with the provided specification.")
            .required(true)
            .allowableValues(CARDINALITY, CHAINR, DEFAULTR, MODIFIER_DEFAULTR, MODIFIER_DEFINER, MODIFIER_OVERWRITER, REMOVR, SHIFTR, SORTR, CUSTOMR)
            .defaultValue(CHAINR.getValue())
            .build();

    public static final PropertyDescriptor JOLT_SPEC_BODY = new PropertyDescriptor.Builder()
            .name("jolt-spec")
            .displayName("Jolt Specification Body")
            .description("Jolt Specification for transform of JSON data. This value is ignored if the Jolt Sort Transformation is selected.  ")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor JOLT_SPEC_FILE = new PropertyDescriptor.Builder()
            .name("jolt-spec-file")
            .displayName("Jolt Specification File")
            .description("Path to Jolt Spec file for transform of JSON data. This value is ignored if the Jolt Sort Transformation is selected.  Only one of Spec File or Spec Body may be used")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(new StandardValidators.FileExistsValidator(true))
            .required(false)
            .build();

    public static final PropertyDescriptor CUSTOM_CLASS = new PropertyDescriptor.Builder()
            .name("jolt-custom-class")
            .displayName("Custom Transformation Class Name")
            .description("Fully Qualified Class Name for Custom Transformation")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MODULES = new PropertyDescriptor.Builder()
            .name("jolt-custom-modules")
            .displayName("Custom Module Directory")
            .description("Comma-separated list of paths to files and/or directories which contain modules containing custom transformations (that are not included on NiFi's classpath).")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor TRANSFORM_CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("Transform Cache Size")
            .description("Compiling a Jolt Transform can be fairly expensive. Ideally, this will be done only once. However, if the Expression Language is used in the transform, we may need "
                + "a new Transform for each FlowFile. This value controls how many of those Transforms we cache in memory in order to avoid having to compile the Transform each time.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
            .required(true)
            .build();

    public static final PropertyDescriptor PRETTY_PRINT = new PropertyDescriptor.Builder()
            .name("pretty_print")
            .displayName("Pretty Print")
            .description("Apply pretty print formatting to the output of the Jolt transform")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The FlowFile with transformed content will be routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile fails processing for any reason (for example, the FlowFile is not valid JSON), it will be routed to this relationship")
            .build();

    private static final List<PropertyDescriptor> properties;
    private static final Set<Relationship> relationships;
    private volatile ClassLoader customClassLoader;
    private static final String DEFAULT_CHARSET = "UTF-8";

    // Cache is guarded by synchronizing on 'this'.
    private volatile int maxTransformsToCache = 10;
    private final Map<String, JoltTransform> transformCache = new LinkedHashMap<String, JoltTransform>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, JoltTransform> eldest) {
            final boolean evict = size() > maxTransformsToCache;
            if (evict) {
                getLogger().debug("Removing Jolt Transform from cache because cache is full");
            }
            return evict;
        }
    };

    static {
        final List<PropertyDescriptor> _properties = new ArrayList<>();
        _properties.add(JOLT_TRANSFORM);
        _properties.add(CUSTOM_CLASS);
        _properties.add(MODULES);
        _properties.add(JOLT_SPEC_BODY);
        _properties.add(JOLT_SPEC_FILE);
        _properties.add(TRANSFORM_CACHE_SIZE);
        _properties.add(PRETTY_PRINT);
        properties = Collections.unmodifiableList(_properties);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * Validate that a Jolt Spec Body is Valid.
     * @param joltSpecBody PropertyValue for a joltSpecBody
     * @return True/False if a Jolt Spec Body is Valid.
     */
    private boolean isJoltSpecBodyPresent(PropertyValue joltSpecBody ) {
        if ( !joltSpecBody.isSet() ) {
            return false;
        }
        if ( StringUtils.isEmpty(joltSpecBody.getValue() )) {
            return false;
        }
        return true;
    }

    private final ClassLoader getClassLoaderForCustomR(String modulePath){
        if (modulePath != null) {
            try {
                return ClassLoaderUtils.getCustomClassLoader(modulePath, this.getClass().getClassLoader(), getJarFilenameFilter());
            } catch (MalformedURLException e) {
                getLogger().warn("Module path failed to load.", e);
                return null;
            }
        } else {
            return this.getClass().getClassLoader();
        }
    }

    private boolean validateCustomModulePath(String modulePath) {
        if (getClassLoaderForCustomR(modulePath) == null) {
            return false;
        } else {
            return true;
        }

    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(validationContext));

        final String transformType = validationContext.getProperty(JOLT_TRANSFORM).getValue();
        final String customTransformType = validationContext.getProperty(CUSTOM_CLASS).getValue();
        final String modulePath = validationContext.getProperty(MODULES).isSet() ? validationContext.getProperty(MODULES).getValue() : null;

        Object specJson= null;

        //Validation #1:
        // (1) The SORTR Transformation Type does not require a Jolt Spec
        // (2) All other Transformation Types require a Jolt Spec
        if (SORTR.getValue().equals( transformType )) {
            //As documented, any specification set is ignored, so no further validation needed.
            return validationResults;
        }

        //Validation #2:
        // All other transformTypes require that a Jolt Spec be present
        // Validate a Jolt Spec Body is present
        if (!isJoltSpecBodyPresent(validationContext.getProperty(JOLT_SPEC_BODY))) {
            validationResults.add(new ValidationResult.Builder().valid(false).subject(JOLT_SPEC_BODY.getDisplayName()).explanation("Jolt specification required for the selected transformation type.").build());
            return validationResults;
        }

        //Validation #3:
        // Validate the Jolt Spec is syntactically correct and do not try to resolve Expression Language variables since they may not exist yet
        final String specValue = validationContext.getProperty(JOLT_SPEC_BODY).getValue();
        specJson = JsonUtils.jsonToObject(specValue.replaceAll("\\$\\{","\\\\\\\\\\$\\{"), DEFAULT_CHARSET);
        try {
            TransformFactory.getTransform(customClassLoader, transformType, specJson);
        } catch (Exception e) {
            validationResults.add(new ValidationResult.Builder().valid(false).subject(JOLT_SPEC_BODY.getDisplayName()).explanation("Jolt specification is syntactically incorrect: " + e.getMessage()).build());
        }

        //Validation #4:
        // Validate Expression Language use in the Jolt Spec Body
        if (validationContext.isExpressionLanguagePresent(specValue)) {
            final String invalidExpressionMsg = validationContext.newExpressionLanguageCompiler().validateExpression(specValue,true);
            if (!StringUtils.isEmpty(invalidExpressionMsg)) {
                validationResults.add(new ValidationResult.Builder().valid(false)
                        .subject(JOLT_SPEC_BODY.getDisplayName())
                        .explanation("Invalid Expression Language: " + invalidExpressionMsg)
                        .build());
            }
        }

        //Validation #5:
        // (1) Custom Transformation Class Name is set, as it is required.
        // (2) Module Path(s) can be loaded (optional)
        // (3) Custom Transformation can be found on the class path.
        if (CUSTOMR.getValue().equals(transformType)) {
            // (1) Custom Transformation Class Name, as it is required.
            if (StringUtils.isEmpty(customTransformType)) {
                final String customMessage = "A custom transformation class should be provided.";
                validationResults.add(new ValidationResult.Builder().valid(false).subject(CUSTOMR.getDisplayName()).explanation(customMessage).build());
                return validationResults;
            }

            // (2) Module Path(s) can be loaded (optional)
            if (!validateCustomModulePath(modulePath)) {
                validationResults.add(new ValidationResult.Builder().valid(false).subject(CUSTOMR.getDisplayName()).explanation("Module path failed to load.").build());
                return validationResults;
            }


            // (3) Custom Transformation can be found on the class path.
            final ClassLoader customClassLoader = getClassLoaderForCustomR(modulePath);
            if (customClassLoader != null){
                try {
                    TransformFactory.getCustomTransform(customClassLoader, customTransformType, specJson);
                } catch (Exception ex) {
                    validationResults.add(new ValidationResult.Builder().valid(false).subject(CUSTOMR.getDisplayName()).explanation("Module path failed to load: " + ex.getMessage()).build());
                    return validationResults;
                }
            }
        }

        return validationResults;
    }

    @Override
    public void onTrigger(final ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final StopWatch stopWatch = new StopWatch(true);

        // Parse FlowFile into JSON
        final Object inputJson;
        try (final InputStream in = session.read(original)) {
            inputJson = JsonUtils.jsonToObject(in);
        } catch (final Exception e) {
            logger.error("Failed to parse JSON {}; routing to failure", new Object[] {original, e});
            session.transfer(original, REL_FAILURE);
            return;
        }

        // Load Jolt Transform
        final JoltTransform transform;
        try {
            transform = getTransform(context, original);
        } catch (final Exception ex) {
            logger.error("Unable to load transform {} due to {}", new Object[] {original, ex.toString(), ex});
            session.transfer(original, REL_FAILURE);
            return;
        }

        // Transform Data
        final String jsonString;
        final ClassLoader originalContextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            //Load Custom Class Loader for Custom Modules
            if (customClassLoader != null) {
                Thread.currentThread().setContextClassLoader(customClassLoader);
            }

            final Object transformedJson = TransformUtils.transform(transform, inputJson);
            jsonString = context.getProperty(PRETTY_PRINT).asBoolean() ? JsonUtils.toPrettyJsonString(transformedJson) : JsonUtils.toJsonString(transformedJson);
        } catch (final Exception ex) {
            logger.error("Unable to transform {} due to {}", new Object[] {original, ex.toString(), ex});
            session.transfer(original, REL_FAILURE);
            return;
        } finally {
            if (customClassLoader != null && originalContextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(originalContextClassLoader);
            }
        }

        FlowFile transformed = session.write(original, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write(jsonString.getBytes(DEFAULT_CHARSET));
            }
        });

        final String transformType = context.getProperty(JOLT_TRANSFORM).getValue();
        transformed = session.putAttribute(transformed, CoreAttributes.MIME_TYPE.key(), "application/json");
        session.transfer(transformed, REL_SUCCESS);
        session.getProvenanceReporter().modifyContent(transformed,"Modified With " + transformType ,stopWatch.getElapsed(TimeUnit.MILLISECONDS));
        logger.info("Transformed {}", new Object[]{original});
    }

    public JoltTransform getTransform(final ProcessContext context, final FlowFile flowFile) throws Exception {

        final String transformType = context.getProperty(JOLT_TRANSFORM).getValue();
        final String customTransformType = context.getProperty(CUSTOM_CLASS).getValue();
        final String modulePath = context.getProperty(MODULES).isSet() ? context.getProperty(MODULES).getValue() : null;

        JoltTransform transform = null;

        //Transform Order of Operations #1:
        // (1) The SORTR Transformation Type does not require a Jolt Spec
        // (2) All other Transformation Types require a Jolt Spec
        if (SORTR.getValue().equals( transformType )) {
            //As documented, any specification set is ignored, so no further validation needed.
            //TODO: Need to figure out how the SORTR transform is loaded.  Is it a direct class transform?
            //TODO: Remove validationResults from this.  Remember - it's already valid.  Just loading based on order of operations
            // of the input of the text boxes.
            //TODO: Order of operations should be added to the Usage Guide for this processor.

            transform = TransformFactory.getTransform(customClassLoader, context.getProperty(JOLT_TRANSFORM).getValue(), null);
            return transform;
        }

        //Transform Order of Operations #2:
        // All transformation specs, other than SORTR, require a Jolt Spec to be present
        final String specString;
        if (isJoltSpecBodyPresent(context.getProperty(JOLT_SPEC_BODY))) {
            specString = context.getProperty(JOLT_SPEC_BODY).evaluateAttributeExpressions(flowFile).getValue();
        } else {
            specString = null;
        }

        // Get the transform from our cache, if it exists, Otherwise create the Transform.
        synchronized (this) {
            transform = transformCache.get(specString);
        }

        //Short-circuit our processing if the transform already exists to save time.
        if (transform != null) {
            return transform;
        }

        //Create the transformation (no need to validate, validation has already occured)
        final Object specJson = JsonUtils.jsonToObject(specString, DEFAULT_CHARSET);

        //If it's a custom module, then grab it, otherwise load up the selected jolt transform.
        if (CUSTOMR.getValue().equals(context.getProperty(JOLT_TRANSFORM).getValue())) {
            transform = TransformFactory.getCustomTransform(customClassLoader, context.getProperty(CUSTOM_CLASS).getValue(), specJson);
        } else {
            transform = TransformFactory.getTransform(customClassLoader, context.getProperty(JOLT_TRANSFORM).getValue(), specJson);
        }

        // Check again for the transform in our cache, since it's possible that another thread has
        // already populated it. If absent from the cache, populate the cache. Otherwise, use the
        // value from the cache.
        // TODO: Should be storing a hash of the specString
        synchronized (this) {
            final JoltTransform existingTransform = transformCache.get(specString);
            if (existingTransform == null) {
                transformCache.put(specString, transform);
                return transform;
            } else {
                return existingTransform;
            }
        }

    }

    @OnScheduled
    public synchronized void setup(final ProcessContext context) {
        transformCache.clear();
        maxTransformsToCache = context.getProperty(TRANSFORM_CACHE_SIZE).asInteger();

        try {
            if (context.getProperty(MODULES).isSet()) {
                customClassLoader = ClassLoaderUtils.getCustomClassLoader(context.getProperty(MODULES).getValue(), this.getClass().getClassLoader(), getJarFilenameFilter());
            } else {
                customClassLoader = this.getClass().getClassLoader();
            }
        } catch (final Exception ex) {
            getLogger().error("Unable to setup processor", ex);
        }
    }

    protected FilenameFilter getJarFilenameFilter() {
        return (dir, name) -> (name != null && name.endsWith(".jar"));
    }

}
