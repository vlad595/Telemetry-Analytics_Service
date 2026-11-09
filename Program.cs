using Telemetry_AnalyticsService;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<MetricsIngestionWorker>();

var host = builder.Build();
host.Run();
