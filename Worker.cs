using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using ClickHouse.Client.Copy;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using ClickHouse.Client.ADO;

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

public class MetricsIngestionWorker : BackgroundService
{
    private readonly IConfiguration _config;
    private readonly ILogger<MetricsIngestionWorker> _logger;
    private readonly Channel<MetricPayload> _channel;
    private const int BatchSize = 1000;

    public MetricsIngestionWorker(IConfiguration config, ILogger<MetricsIngestionWorker> logger)
    {
        _config = config;
        _logger = logger;
        _channel = Channel.CreateBounded<MetricPayload>(new BoundedChannelOptions(50000)
        {
            FullMode = BoundedChannelFullMode.Wait
        });
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
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
                    await _channel.Writer.WriteAsync(payload, stoppingToken);
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
        var buffer = new List<MetricPayload>(BatchSize);

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

    private async Task BulkInsertToClickHouseAsync(List<MetricPayload> metrics, CancellationToken token)
    {
        try
        {
            using var connection = new ClickHouseConnection(_config.GetConnectionString("ClickHouse"));
            using var bulkCopy = new ClickHouseBulkCopy(connection)
            {
                DestinationTableName = "SystemMetrics",
                BatchSize = metrics.Count
            };

            await bulkCopy.InitAsync();

            var rows = metrics.Select(m => new object[] 
            { 
                m.AgentId, m.Timestamp, m.CpuUsage, m.MemoryUsage, m.DiskSpace, m.NetworkRxBytes, m.NetworkTxBytes 
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