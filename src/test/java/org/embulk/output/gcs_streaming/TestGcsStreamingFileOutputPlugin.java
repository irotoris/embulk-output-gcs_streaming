package org.embulk.output.gcs_streaming;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;

import org.embulk.output.gcs_streaming.GcsStreamingFileOutputPlugin.PluginTask;

import org.embulk.spi.Buffer;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputRunner;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalFileOutput;
import org.embulk.standards.CsvParserPlugin;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNotNull;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class TestGcsStreamingFileOutputPlugin
{
    private static String gcpProjectId;
    private static String gcpBucket;
    private static Optional<String> gcpJsonKeyfile;
    private static String localPathPrefix;
    private FileOutputRunner runner;
    /*
     * This test case requires environment variables
     *   GCP_PROJECT_ID
     *   GCP_BUCKET
     *   GCP_JSON_KEYFILE
     * And prepare gcloud authentication ADC, following command.
     *   $ gcloud auth application-default login
     */
    @BeforeClass
    public static void initializeConstant()
    {
        gcpProjectId = System.getenv("GCP_PROJECT_ID");
        gcpJsonKeyfile = Optional.of(System.getenv("GCP_JSON_KEYFILE"));
        gcpBucket = System.getenv("GCP_BUCKET");
        localPathPrefix = Resources.getResource("test.000.csv").getPath();
        assumeNotNull(gcpJsonKeyfile, gcpProjectId, gcpBucket, localPathPrefix);
        // skip test cases, if environment variables are not set.
    }

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    private GcsStreamingFileOutputPlugin plugin;

    @Before
    public void createResources()
    {
        plugin = new GcsStreamingFileOutputPlugin();
        runner = new FileOutputRunner(runtime.getInstance(GcsStreamingFileOutputPlugin.class));
    }

    @Test
    public void checkDefaultValues()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("in", inputConfig())
                .set("parser", parserConfig(schemaConfig()))
                .set("type", "gcs_streaming")
                .set("project_id", gcpProjectId)
                .set("bucket", gcpBucket)
                .set("json_keyfile", gcpJsonKeyfile)
                .set("path_prefix", "tests/data")
                .set("file_ext", ".csv")
                .set("formatter", formatterConfig());

        PluginTask task = config.loadConfig(PluginTask.class);

        assertEquals(gcpProjectId, task.getProjectId());
        assertEquals(gcpBucket, task.getBucket());
        assertEquals(gcpJsonKeyfile, task.getJsonKeyfile());
        assertEquals(".%03d.%02d.", task.getSequenceFormat());
        assertEquals("application/octet-stream", task.getContentType());
        assertEquals("tests/data", task.getPathPrefix());
        assertEquals(".csv", task.getFileNameExtension());
    }

    @Test
    public void testTransaction()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("in", inputConfig())
                .set("parser", parserConfig(schemaConfig()))
                .set("type", "gcs_streaming")
                .set("project_id", gcpProjectId)
                .set("bucket", gcpBucket)
                .set("path_prefix", "tests/data")
                .set("file_ext", "csv")
                .set("formatter", formatterConfig());

        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();

        runner.transaction(config, schema, 0, new Control());
    }

    @Test
    public void testTransactionWithJsonKeyfile()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("in", inputConfig())
                .set("parser", parserConfig(schemaConfig()))
                .set("type", "gcs_streaming")
                .set("project_id", gcpProjectId)
                .set("bucket", gcpBucket)
                .set("json_keyfile", gcpJsonKeyfile)
                .set("path_prefix", "tests/data")
                .set("file_ext", "csv")
                .set("formatter", formatterConfig());

        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();

        runner.transaction(config, schema, 0, new Control());
    }

    @Test
    public void testResume()
    {
        // no support resume
    }

    @Test
    public void testCleanup()
    {
        PluginTask task = config().loadConfig(PluginTask.class);
        plugin.cleanup(task.dump(), 0, Lists.newArrayList()); // no errors happens
    }

    @Test
    public void testGcsFileOutputByOpen() throws Exception
    {
        ConfigSource configSource = config();
        PluginTask task = configSource.loadConfig(PluginTask.class);
        Schema schema = configSource.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        runner.transaction(configSource, schema, 0, new Control());

        TransactionalFileOutput output = plugin.open(task.dump(), 0);

        output.nextFile();

        FileInputStream is = new FileInputStream(localPathPrefix);
        byte[] bytes = convertInputStreamToByte(is);
        Buffer buffer = Buffer.wrap(bytes);
        output.add(buffer);

        output.finish();
        output.commit();

        assertRecords(getFileContentsFromGcs(task));
    }

    public ConfigSource config()
    {
        return Exec.newConfigSource()
                .set("in", inputConfig())
                .set("parser", parserConfig(schemaConfig()))
                .set("type", "gcs_streaming")
                .set("project_id", gcpProjectId)
                .set("bucket", gcpBucket)
                .set("path_prefix", "tests/data")
                .set("file_ext", "csv")
                .set("formatter", formatterConfig());
    }

    private class Control implements OutputPlugin.Control
    {
        @Override
        public List<TaskReport> run(TaskSource taskSource)
        {
            return Lists.newArrayList(Exec.newTaskReport());
        }
    }

    private ImmutableMap<String, Object> inputConfig()
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "file");
        builder.put("path_prefix", localPathPrefix);
        builder.put("last_path", "");
        return builder.build();
    }

    private ImmutableMap<String, Object> parserConfig(ImmutableList<Object> schemaConfig)
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "csv");
        builder.put("newline", "CRLF");
        builder.put("delimiter", ",");
        builder.put("quote", "\"");
        builder.put("escape", "\"");
        builder.put("trim_if_not_quoted", false);
        builder.put("skip_header_lines", 1);
        builder.put("allow_extra_columns", false);
        builder.put("allow_optional_columns", false);
        builder.put("columns", schemaConfig);
        return builder.build();
    }

    private ImmutableList<Object> schemaConfig()
    {
        ImmutableList.Builder<Object> builder = new ImmutableList.Builder<>();
        builder.add(ImmutableMap.of("name", "id", "type", "long"));
        builder.add(ImmutableMap.of("name", "account", "type", "string"));
        builder.add(ImmutableMap.of("name", "ts", "type", "timestamp", "format", "%Y-%m-%d %H:%M:%S"));
        builder.add(ImmutableMap.of("name", "dt", "type", "timestamp", "format", "%Y%m%d"));
        builder.add(ImmutableMap.of("name", "message", "type", "string"));
        return builder.build();
    }

    private ImmutableMap<String, Object> formatterConfig()
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "csv");
        builder.put("header_line", true);
        return builder.build();
    }

    private void assertRecords(ImmutableList<List<String>> records)
    {
        assertEquals(3, records.size());
        {
            List<String> record = records.get(1);
            assertEquals("1", record.get(0));
            assertEquals("account1", record.get(1));
            assertEquals("2020-01-01 00:00:00", record.get(2));
            assertEquals("20200101", record.get(3));
            assertEquals("init", record.get(4));
        }

        {
            List<String> record = records.get(2);
            assertEquals("2", record.get(0));
            assertEquals("account2", record.get(1));
            assertEquals("2020-02-01 12:00:00", record.get(2));
            assertEquals("20200201", record.get(3));
            assertEquals("init", record.get(4));
        }
    }

    private ImmutableList<List<String>> getFileContentsFromGcs(PluginTask task) throws Exception
    {
        Storage storage = StorageOptions.newBuilder()
                .setProjectId(task.getProjectId())
                .build()
                .getService();

        String blobName = task.getPathPrefix() + String.format(task.getSequenceFormat(), 0, 0) + task.getFileNameExtension();
        BlobId blobId = BlobId.of(task.getBucket(), blobName);
        Blob blob = storage.get(blobId);
        byte[] byteContent = blob.getContent(); // one or multiple RPC calls will be issued
        String strContent = new String(byteContent);

        ImmutableList.Builder<List<String>> builder = new ImmutableList.Builder<>();

        String line;
        BufferedReader reader = new BufferedReader(new StringReader(strContent));
        while ((line = reader.readLine()) != null) {
            List<String> records = Arrays.asList(line.split(",", 0));
            builder.add(records);
        }
        return builder.build();
    }

    private byte[] convertInputStreamToByte(InputStream is) throws IOException
    {
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        byte [] buffer = new byte[1024];
        while (true) {
            int len = is.read(buffer);
            if (len < 0) {
                break;
            }
            bo.write(buffer, 0, len);
        }
        return bo.toByteArray();
    }
}
