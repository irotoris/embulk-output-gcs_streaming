package org.embulk.output.gcs_streaming;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Buffer;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.TransactionalFileOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

public class GcsStreamingFileOutputPlugin
        implements FileOutputPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("bucket")
        public String getBucket();

        @Config("path_prefix")
        public String getPathPrefix();

        @Config("file_ext")
        public String getFileNameExtension();

        @Config("sequence_format")
        @ConfigDefault("\".%03d.%02d.\"")
        public String getSequenceFormat();

        @Config("project_id")
        public String getProjectId();

        @Config("content_type")
        @ConfigDefault("\"application/octet-stream\"")
        public String getContentType();

        @Config("json_keyfile")
        @ConfigDefault("null")
        public Optional<String> getJsonKeyfile();
    }

    private static final Logger logger = LoggerFactory.getLogger(GcsStreamingFileOutputPlugin.class);

    @Override
    public ConfigDiff transaction(ConfigSource config, int taskCount,
                                  FileOutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        // retryable (idempotent) output:
        // return resume(task.dump(), taskCount, control);

        // non-retryable (non-idempotent) output:
        control.run(task.dump());
        return Exec.newConfigDiff();
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             int taskCount,
                             FileOutputPlugin.Control control)
    {
        throw new UnsupportedOperationException("gcs_streaming output plugin does not support resuming");
    }

    @Override
    public void cleanup(TaskSource taskSource,
            int taskCount,
            List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TransactionalFileOutput open(TaskSource taskSource, final int taskIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        final String pathPrefix = task.getPathPrefix();
        final String pathSuffix = task.getFileNameExtension();
        final String sequenceFormat = task.getSequenceFormat();
        final String gcpProjectId = task.getProjectId();
        final String gcsBucketName = task.getBucket();
        final String gcsObjectContentType = task.getContentType();
        final Optional<String> gcpJsonKeyfile = task.getJsonKeyfile();
        Storage storage = new GcsClient(gcpProjectId, gcpJsonKeyfile).storage;

        return new TransactionalFileOutput()
        {
            private int fileIndex = 0;
            private WriteChannel output = null;

            public void nextFile()
            {
                closeFile();

                String blobName = pathPrefix + String.format(sequenceFormat, taskIndex, fileIndex) + pathSuffix;
                BlobId blobId = BlobId.of(gcsBucketName, blobName);
                BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType(gcsObjectContentType).build();

                logger.info("Writing gs://{}/{}", gcsBucketName, blobName);
                try {
                    output = storage.writer(blobInfo);
                }
                catch (StorageException ex) {
                    throw new RuntimeException(ex);
                }
                fileIndex++;
            }

            private void closeFile()
            {
                if (output != null) {
                    try {
                        output.close();
                    }
                    catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }

            public void add(Buffer buffer)
            {
                try {
                    output.write(ByteBuffer.wrap(buffer.array(), buffer.offset(), buffer.limit()));
                }
                catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
                finally {
                    buffer.release();
                }
            }

            public void finish()
            {
                closeFile();
            }

            public void close()
            {
                closeFile();
            }

            public void abort()
            {
            }

            public TaskReport commit()
            {
                TaskReport report = Exec.newTaskReport();
                return report;
            }
        };
    }
}
