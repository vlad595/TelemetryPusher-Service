using System;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.SignalR.Client;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace TelemetryHubPusherService;

public class TelemetryPayload 
{
    public string InstanceId { get; set; } = string.Empty;
}
public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IConnectionFactory _factory;
    private readonly IConfiguration _config;
    
    private IConnection? _connection;
    private IChannel? _channel;
    private HubConnection? _hubConnection;

    public Worker(ILogger<Worker> logger, IConnectionFactory factory, IConfiguration config)
    {
        _logger = logger;
        _factory = factory;
        _config = config;
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {

        try
        {
            _connection = await _factory.CreateConnectionAsync();
            _channel = await _connection.CreateChannelAsync();

            var hubUrl = _config["HubUrl"] ?? "localhost";
            _hubConnection = new HubConnectionBuilder().WithUrl(hubUrl).WithAutomaticReconnect().Build();
            await _hubConnection.StartAsync(cancellationToken);
            await base.StartAsync(cancellationToken);
        }catch(Exception ex)
        {
            _logger.LogError(ex, "Error while trying to connect to hub: {Message}", ex.Message);
        }
    }
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_channel == null)
        {
            _logger.LogError("Channel is not initialized. Returning...");
            return;
        }
        if (_hubConnection?.State == HubConnectionState.Disconnected)
        {
            return;
        }
        var consumer = new AsyncEventingBasicConsumer(_channel);

        consumer.ReceivedAsync += async (model, ea) =>
        {
            _logger.LogInformation("Received metrics from queue");
            var body = ea.Body.ToArray();
            var jsonString = Encoding.UTF8.GetString(body);

            try{
                var payload = JsonSerializer.Deserialize<TelemetryPayload>(jsonString, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                if (!string.IsNullOrEmpty(payload?.InstanceId)){
                    
                    if (_hubConnection.State == HubConnectionState.Connected){
                        _logger.LogInformation("Trying to send to hub...");
                        await _hubConnection.InvokeAsync("SendMetrics", payload.InstanceId, jsonString);
                    }
                    else{
                        
                    }
                }
                await _channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);

            }catch(JsonException ex){
                _logger.LogError(ex, "Json error: {Message}", ex.Message);
                await _channel.BasicAckAsync(ea.DeliveryTag, false);
            }catch(Exception ex){
                _logger.LogError(ex, "Error: {Message}", ex.Message);
                await _channel.BasicNackAsync(ea.DeliveryTag, false, true);
            }
        };
        await _channel.BasicConsumeAsync("metrics_queue", false, consumer);
    }
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await _hubConnection.StopAsync(cancellationToken);
        await base.StopAsync(cancellationToken);
    }
    public override void Dispose()
    {
        _channel?.CloseAsync();
        _channel?.Dispose();
        _connection?.CloseAsync();
        _connection?.Dispose();
        _hubConnection?.DisposeAsync();
        base.Dispose();
    }
}
