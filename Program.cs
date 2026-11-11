using Services;
using Telemetry_AnalyticsService;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddMemoryCache();
builder.Services.AddSingleton<IUserSubscriptionHandler, UserSubscriptionHandler>();
builder.Services.AddHostedService<MetricsIngestionWorker>();

var host = builder.Build();
host.Run();
