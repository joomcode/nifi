package org.apache.nifi.processors.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.util.orc.FlowfileFileSystem;
import org.apache.nifi.util.orc.JSONToOrcConverter;
import org.apache.nifi.util.orc.JsonReader;
import org.apache.orc.*;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.hive.HiveJdbcCommon;
import org.apache.nifi.util.hive.HiveUtils;
import org.apache.orc.Writer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by sterligovak on 21.04.17.
 */
@SideEffectFree
@Tags({"orc", "hive", "convert", "json"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "Sets the mime type to application/octet-stream"),
        @WritesAttribute(attribute = "filename", description = "Sets the filename to the existing filename with the extension replaced by / added to by .orc"),
        @WritesAttribute(attribute = "record.count", description = "Sets the number of records in the ORC file."),
})
public class ConvertJSONToORC extends AbstractProcessor {

    public static final String ORC_MIME_TYPE = "application/octet-stream";
    public static final String RECORD_COUNT_ATTRIBUTE = "record.count";

    // Properties
    public static final PropertyDescriptor ORC_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder()
            .name("orc-config-resources")
            .displayName("ORC Configuration Resources")
            .description("A file or comma separated list of files which contains the ORC configuration (hive-site.xml, e.g.). Without this, Hadoop "
                    + "will search the classpath for a 'hive-site.xml' file or will revert to a default configuration. Please see the ORC documentation for more details.")
            .required(false)
            .expressionLanguageSupported(false)
            .addValidator(HiveUtils.createMultipleFilesExistValidator())
            .build();

    public static final PropertyDescriptor STRIPE_SIZE = new PropertyDescriptor.Builder()
            .name("orc-stripe-size")
            .displayName("Stripe Size")
            .description("The size of the memory buffer (in bytes) for writing stripes to an ORC file")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("64 MB")
            .build();

    public static final PropertyDescriptor BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("orc-buffer-size")
            .displayName("Buffer Size")
            .description("The maximum size of the memory buffers (in bytes) used for compressing and storing a stripe in memory. This is a hint to the ORC writer, "
                    + "which may choose to use a smaller buffer size based on stripe size and number of columns for efficient stripe writing and memory utilization.")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("256 KB")
            .build();

    public static final PropertyDescriptor COMPRESSION_TYPE = new PropertyDescriptor.Builder()
            .name("orc-compression-type")
            .displayName("Compression Type")
            .required(true)
            .expressionLanguageSupported(false)
            .allowableValues("NONE", "ZLIB", "SNAPPY", "LZO")
            .defaultValue("NONE")
            .build();

    public static final PropertyDescriptor ORC_SCHEMA = new PropertyDescriptor.Builder()
            .name("orc-schema")
            .displayName("ORC Schema")
            .description("Schema of the ORC. Example: ")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor ORC_BLOOM_FILTER_COLUMNS = new PropertyDescriptor.Builder()
            .name("orc-bloom-filter-columns")
            .displayName("ORC Bloom filter columns")
            .description("Comma separated columns to compute Bloom filters")
            .required(false)
            .expressionLanguageSupported(false)
            .defaultValue(null)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor ORC_BLOOM_FILTER_FPP = new PropertyDescriptor.Builder()
            .name("orc-bloom-filter-fpp")
            .displayName("ORC Bloom filter false positive percent")
            .description("Comma separated columns to compute Bloom filters")
            .required(true)
            .expressionLanguageSupported(false)
            .defaultValue("0.05")
            .addValidator(new Validator() {
                @Override
                public ValidationResult validate(String subject, String value, ValidationContext context) {
                    if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(value)) {
                        return new ValidationResult.Builder().subject(subject).input(value).explanation("Expression Language Present").valid(true).build();
                    }

                    String reason = null;
                    try {
                        final double doubleVal = Double.parseDouble(value);

                        if (doubleVal <= 0) {
                            reason = "not a positive value";
                        }
                    } catch (final NumberFormatException e) {
                        reason = "not a valid double";
                    }

                    return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
                }
            })
            .build();

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The FlowFile containing ORC file")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be parsed as JSON or cannot be converted to ORC for any reason")
            .build();

    private final static List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;

    private static class Settings {
        Configuration orcConfig = new Configuration();
        int maxEntries;
        long stripeSize;
        int bufferSize;
        String bloomFilterColumns;
        double bloomFilterFpp;
        CompressionKind compressionType;
        TypeDescription orcSchema;
        JSONToOrcConverter converter;
    }

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(ORC_CONFIGURATION_RESOURCES);
        _propertyDescriptors.add(STRIPE_SIZE);
        _propertyDescriptors.add(BUFFER_SIZE);
        _propertyDescriptors.add(COMPRESSION_TYPE);
        _propertyDescriptors.add(ORC_SCHEMA);
        _propertyDescriptors.add(ORC_BLOOM_FILTER_COLUMNS);
        _propertyDescriptors.add(ORC_BLOOM_FILTER_FPP);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    private final AtomicReference<Settings> settingsRef = new AtomicReference<>();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void setup(ProcessContext context) {
        Settings newSettings = new Settings();

        boolean confFileProvided = context.getProperty(ORC_CONFIGURATION_RESOURCES).isSet();
        if (confFileProvided) {
            final String configFiles = context.getProperty(ORC_CONFIGURATION_RESOURCES).getValue();
            newSettings.orcConfig = HiveJdbcCommon.getConfigurationFromFiles(configFiles);
        }

        newSettings.stripeSize = context.getProperty(STRIPE_SIZE).asDataSize(DataUnit.B).longValue();
        newSettings.bufferSize = context.getProperty(BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        newSettings.bloomFilterColumns = context.getProperty(ORC_BLOOM_FILTER_COLUMNS).getValue();
        newSettings.bloomFilterFpp = context.getProperty(ORC_BLOOM_FILTER_FPP).asDouble();
        newSettings.compressionType = CompressionKind.valueOf(context.getProperty(COMPRESSION_TYPE).getValue());

        String schema = context.getProperty(ORC_SCHEMA)
                .getValue()
                .replaceAll("\\s+", "");
        newSettings.orcSchema = TypeDescription.fromString(schema);
        newSettings.converter = new JSONToOrcConverter(newSettings.orcSchema);
        settingsRef.set(newSettings);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        Settings settings = settingsRef.get();
        final String orcFileName = flowFile.getAttribute(CoreAttributes.FILENAME.key());

        long startTime = System.currentTimeMillis();
        final AtomicInteger totalRecordCount = new AtomicInteger(0);

        try {
            flowFile = session.write(flowFile, (rawIn, rawOut) -> {
                try (InputStream in = new BufferedInputStream(rawIn);
                     OutputStream out = new BufferedOutputStream(rawOut)) {
                    Writer writer = OrcFile.createWriter(
                            new Path(orcFileName),
                            OrcFile.writerOptions(settings.orcConfig)
                                    .setSchema(settings.orcSchema)
                                    .stripeSize(settings.stripeSize)
                                    .bufferSize(settings.bufferSize)
                                    .compress(settings.compressionType)
                                    .bloomFilterColumns(settings.bloomFilterColumns)
                                    .bloomFilterFpp(settings.bloomFilterFpp)
                                    .fileSystem(new FlowfileFileSystem(out))
                    );

                    VectorizedRowBatch batch = settings.orcSchema.createRowBatch();
                    RecordReader reader = new JsonReader(in, settings.converter);
                    try {
                        int recordCount = 0;
                        while (reader.nextBatch(batch)) {
                            writer.addRowBatch(batch);
                            recordCount += batch.size;
                        }
                        totalRecordCount.addAndGet(recordCount);
                    } finally {
                        reader.close();
                        writer.close();
                    }
                }
            });

            flowFile = session.putAttribute(flowFile, RECORD_COUNT_ATTRIBUTE, Integer.toString(totalRecordCount.get()));
            StringBuilder newFilename = new StringBuilder();
            int extensionIndex = orcFileName.lastIndexOf(".");
            if (extensionIndex != -1) {
                newFilename.append(orcFileName.substring(0, extensionIndex));
            } else {
                newFilename.append(orcFileName);
            }
            newFilename.append(".orc");
            flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), ORC_MIME_TYPE);
            flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), newFilename.toString());

            session.transfer(flowFile, REL_SUCCESS);
            session.getProvenanceReporter().modifyContent(flowFile, "Converted " + totalRecordCount.get() + " records", System.currentTimeMillis() - startTime);
        } catch (Exception e) {
            getLogger().error("Failed to convert {} from JSON to ORC due to {}; transferring to failure", new Object[]{flowFile, e}, e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
