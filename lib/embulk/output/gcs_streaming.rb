Embulk::JavaPlugin.register_output(
  "gcs_streaming", "org.embulk.output.gcs_streaming.GcsStreamingFileOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
