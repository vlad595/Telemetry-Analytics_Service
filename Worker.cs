using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using ClickHouse.Client.Copy;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using ClickHouse.Client.ADO;
using Services;

namespace Telemetry_AnalyticsService;

public record MetricPayload(
    [property: JsonPropertyName("instanceId")] Guid AgentId, 
    [property: JsonPropertyName("Timestamp")] DateTime Timestamp, 
    [property: JsonPropertyName("CpuLoad")] double CpuUsage, 
    [property: JsonPropertyName("RamLoad")] double MemoryUsage, 
    [property: JsonPropertyName("FreeDiskSpace")] double DiskSpace,
    [property: JsonPropertyName("WiFiRx")] double NetworkRxBytes,
    [property: JsonPropertyName("WiFiTx")] double NetworkTxBytes 
);

public record StoredMetric(MetricPayload Metric, DateTime ExpiresAt);

public class MetricsIngestionWorker : BackgroundService
{
    private readonly IConfiguration _config;
    private readonly ILogger<MetricsIngestionWorker> _logger;
    private readonly IUserSubscriptionHandler _subscriptionHandler;
    private readonly Channel<StoredMetric> _channel;
    private const int BatchSize = 1000;

    public MetricsIngestionWorker(
        IConfiguration config,
        ILogger<MetricsIngestionWorker> logger,
        IUserSubscriptionHandler subscriptionHandler)
    {
        _config = config;
        _logger = logger;
        _subscriptionHandler = subscriptionHandler;
        _channel = Channel.CreateBounded<StoredMetric>(new BoundedChannelOptions(50000)
        {
            FullMode = BoundedChannelFullMode.Wait
        });
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await EnsureClickHouseTtlAsync(stoppingToken);

        var factory = new ConnectionFactory { 
            HostName = _config["RabbitMQ:Host"] ?? throw new Exception("No rabbitmq host"),
            UserName = _config["RabbitMQ:username"] ?? throw new Exception("No rabbitmq username"),
            Password = _config["RabbitMQ:password"] ?? throw new Exception("No rabbitmq password")
        };
        using var connection = await factory.CreateConnectionAsync();
        using var rmqChannel = await connection.CreateChannelAsync();

        await rmqChannel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1000, global: false);

        var consumer = new AsyncEventingBasicConsumer(rmqChannel);

        consumer.ReceivedAsync += async (model, ea) =>
        {
            try
            {
                var body = ea.Body.ToArray();
                var payload = JsonSerializer.Deserialize<MetricPayload>(body);

                if (payload != null)
                {
                    int subscriptionTier = await _subscriptionHandler.GetSubscriptionTier(payload.AgentId.ToString());
                    int retentionDays = subscriptionTier == 2 ? 1 : 7;
                    var storedMetric = new StoredMetric(payload, payload.Timestamp.AddDays(retentionDays));
                    await _channel.Writer.WriteAsync(storedMetric, stoppingToken);
                }
                await rmqChannel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false); 
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Помилка парсингу повідомлення. Дані: {Json}", System.Text.Encoding.UTF8.GetString(ea.Body.ToArray()));
                

                await rmqChannel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
            }
        };

        await rmqChannel.BasicConsumeAsync(queue: "analytics-metrics_queue", autoAck: false, consumer: consumer);

        await ProcessBatchesAsync(stoppingToken);
    }

    private async Task ProcessBatchesAsync(CancellationToken stoppingToken)
    {
        var buffer = new List<StoredMetric>(BatchSize);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var firstMetric = await _channel.Reader.ReadAsync(stoppingToken);
                buffer.Add(firstMetric);

                using var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                cts.CancelAfter(TimeSpan.FromSeconds(2));

                try
                {
                    while (buffer.Count < BatchSize)
                    {
                        var metric = await _channel.Reader.ReadAsync(cts.Token);
                        buffer.Add(metric);
                    }
                }
                catch (OperationCanceledException)
                {
                    
                }

                if (buffer.Count > 0)
                {
                    await BulkInsertToClickHouseAsync(buffer, stoppingToken);
                    buffer.Clear();
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private async Task EnsureClickHouseTtlAsync(CancellationToken token)
    {
        using var connection = new ClickHouseConnection(_config.GetConnectionString("ClickHouse"));
        await connection.OpenAsync(token);

        using (var addExpiryColumn = connection.CreateCommand())
        {
            addExpiryColumn.CommandText = "ALTER TABLE SystemMetrics ADD COLUMN IF NOT EXISTS ExpiresAt DateTime DEFAULT Timestamp + INTERVAL 7 DAY";
            await addExpiryColumn.ExecuteNonQueryAsync(token);
        }

        using (var configureTtl = connection.CreateCommand())
        {
            configureTtl.CommandText = "ALTER TABLE SystemMetrics MODIFY TTL ExpiresAt";
            await configureTtl.ExecuteNonQueryAsync(token);
        }
    }

    private async Task BulkInsertToClickHouseAsync(List<StoredMetric> metrics, CancellationToken token)
    {
        try
        {
            using var connection = new ClickHouseConnection(_config.GetConnectionString("ClickHouse"));
            using var bulkCopy = new ClickHouseBulkCopy(connection)
            {
                DestinationTableName = "SystemMetrics",
                ColumnNames = new[] { "AgentId", "Timestamp", "CpuUsage", "MemoryUsage", "DiskSpace", "NetworkRxBytes", "NetworkTxBytes", "ExpiresAt" },
                BatchSize = metrics.Count
            };

            await bulkCopy.InitAsync();

            var rows = metrics.Select(m => new object[] 
            { 
                m.Metric.AgentId, m.Metric.Timestamp, m.Metric.CpuUsage, m.Metric.MemoryUsage, m.Metric.DiskSpace, m.Metric.NetworkRxBytes, m.Metric.NetworkTxBytes, m.ExpiresAt
            }).ToArray();

            await bulkCopy.WriteToServerAsync(rows, token);
            _logger.LogInformation($"Збережено {metrics.Count} метрик у ClickHouse.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Помилка запису в ClickHouse");
        }
    }
}
