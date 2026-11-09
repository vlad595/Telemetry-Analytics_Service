using System.Text.Json;
using System.Threading.Channels;
using ClickHouse.Client.Copy;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using ClickHouse.Client.ADO;

public record MetricPayload(
    Guid AgentId, 
    DateTime Timestamp, 
    double CpuUsage, 
    double MemoryUsage, 
    double DiskSpace,
    long NetworkRxBytes,
    long NetworkTxBytes);
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
        var clickHouseTask = ProcessBatchesAsync(stoppingToken);

        var factory = new ConnectionFactory { HostName = _config["RabbitMQ:Host"] ?? throw new Exception("No rabbitmq host") };
        using var connection = await factory.CreateConnectionAsync();
        using var rmqChannel = await connection.CreateChannelAsync();

        var consumer = new AsyncEventingBasicConsumer(rmqChannel);

        consumer.ReceivedAsync += async (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var payload = JsonSerializer.Deserialize<MetricPayload>(body);

            if (payload != null)
            {
                await _channel.Writer.WriteAsync(payload, stoppingToken);
            }

            await rmqChannel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false); 
        };

        await rmqChannel.BasicConsumeAsync(queue: "metrics_queue", autoAck: false, consumer: consumer);

        await clickHouseTask;
    }

    private async Task ProcessBatchesAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(2));
        var buffer = new List<MetricPayload>(BatchSize);

        while (!stoppingToken.IsCancellationRequested)
        {
            await timer.WaitForNextTickAsync(stoppingToken);
            
            while (_channel.Reader.TryRead(out var metric))
            {
                buffer.Add(metric);
                if (buffer.Count >= BatchSize) break;
            }

            if (buffer.Count > 0)
            {
                await BulkInsertToClickHouseAsync(buffer, stoppingToken);
                buffer.Clear();
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