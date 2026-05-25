using Microsoft.AspNetCore.SignalR;
using MQTTnet;
using PeopleCounter_Backend.Data;
using PeopleCounter_Backend.Models;
using System.Buffers;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;

namespace PeopleCounter_Backend.Services
{
    public class MqttMessageProcessor
    {
        private readonly Channel<MqttApplicationMessageReceivedEventArgs> _messageQueue;
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly IHubContext<PeopleCounterHub> _hubContext;
        private readonly ILogger<MqttMessageProcessor> _logger;
        private readonly SensorCacheService _sensorCache;
        private readonly CancellationTokenSource _cts = new();

        private const int BATCH_INTERVAL_MS = 200;
        private const int MAX_BATCH_SIZE = 100;
        private const int QUEUE_WARNING_THRESHOLD = 500;
        private const int QUEUE_MAX_CAPACITY = 5000;
        private const int MAX_LAST_KNOWN_COUNTS = 1000; 
        private int _queueDepth = 0;

        private readonly Dictionary<(string DeviceId, string Location), (int In, int Out)> _lastKnownCounts = new();


        private long _lastSummaryBroadcastTicks = DateTime.MinValue.Ticks;
        private static readonly TimeSpan SummaryDebounceInterval = TimeSpan.FromSeconds(3);

        public MqttMessageProcessor(
            IServiceScopeFactory scopeFactory,
            IHubContext<PeopleCounterHub> hubContext,
            ILogger<MqttMessageProcessor> logger,
            SensorCacheService sensorCache)
        {
            _scopeFactory = scopeFactory;
            _hubContext = hubContext;
            _logger = logger;
            _sensorCache = sensorCache;

            _messageQueue = Channel.CreateBounded<MqttApplicationMessageReceivedEventArgs>(
                new BoundedChannelOptions(QUEUE_MAX_CAPACITY)
                {
                    FullMode = BoundedChannelFullMode.DropOldest,
                    SingleReader = true,
                    SingleWriter = false
                });

            _logger.LogInformation("MqttMessageProcessor starting background processor");
            _ = Task.Run(() => RunProcessorWithRestartAsync(_cts.Token));
        }

        public void EnqueueMessage(MqttApplicationMessageReceivedEventArgs e)
        {
            if (_messageQueue.Writer.TryWrite(e))
            {
                var depth = Interlocked.Increment(ref _queueDepth);

                if (depth > QUEUE_WARNING_THRESHOLD)
                {
                    _logger.LogWarning(
                        "High queue depth: ~{Count} messages. Processing may be falling behind.",
                        depth);
                }
            }
            else
            {
                _logger.LogWarning(
                    "MQTT queue full ({Capacity}). Dropping oldest message to make room.",
                    QUEUE_MAX_CAPACITY);
            }
        }

        public void Stop()
        {
            _logger.LogInformation("Stopping MqttMessageProcessor...");
            _cts.Cancel();
            _messageQueue.Writer.Complete();
        }

        private async Task RunProcessorWithRestartAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await ProcessMessagesAsync(ct);
                    return;
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogCritical(ex, "Message processor crashed. Restarting in 2 seconds...");
                    try { await Task.Delay(2000, ct); } catch (OperationCanceledException) { return; }
                }
            }
        }

        private async Task ProcessMessagesAsync(CancellationToken cancellationToken)
        {
            _logger.LogDebug("Message processor background task started");

            var buffer = new List<MqttApplicationMessageReceivedEventArgs>();

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    buffer.Clear();

                    if (await _messageQueue.Reader.WaitToReadAsync(cancellationToken))
                    {
                        var deadline = DateTime.UtcNow.AddMilliseconds(BATCH_INTERVAL_MS);

                        while (buffer.Count < MAX_BATCH_SIZE && DateTime.UtcNow < deadline)
                        {
                            if (_messageQueue.Reader.TryRead(out var msg))
                            {
                                buffer.Add(msg);
                            }
                            else
                            {
                                await Task.Delay(10, cancellationToken);
                            }
                        }

                        while (buffer.Count < MAX_BATCH_SIZE && _messageQueue.Reader.TryRead(out var msg))
                        {
                            buffer.Add(msg);
                        }

                        if (buffer.Count > 0)
                        {
                            Interlocked.Add(ref _queueDepth, -buffer.Count);
                            _logger.LogDebug("Processing batch of {Count} MQTT messages", buffer.Count);
                            await ProcessMessageBatch(buffer);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    _logger.LogInformation("Message processor cancelled - shutting down gracefully");
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in message processor main loop");
                    await Task.Delay(1000, cancellationToken); 
                }
            }

            _logger.LogInformation("Message processor background task stopped");
        }

        private async Task ProcessMessageBatch(List<MqttApplicationMessageReceivedEventArgs> messages)
        {
            var totalStopwatch = System.Diagnostics.Stopwatch.StartNew();

            try
            {
                var allRecords = new List<PeopleCounter>();

                foreach (var e in messages)
                {
                    try
                    {
                        var payloadSequence = e.ApplicationMessage.Payload;
                        if (payloadSequence.IsEmpty) continue;

                        byte[] payloadBytes = payloadSequence.ToArray();
                        var payload = Encoding.UTF8.GetString(payloadBytes);

                        _logger.LogDebug("MQTT Payload: {Payload}", payload);

                        var mqttMessages = JsonSerializer.Deserialize<List<MqttPayload>>(payload);
                        if (mqttMessages is null || mqttMessages.Count == 0) continue;

                        foreach (var device in mqttMessages)
                        {
                            foreach (var d in device.Data)
                            {
                                if (!DateTime.TryParse(d.TimeStamp, out var ts))
                                {
                                    _logger.LogWarning(
                                        "Invalid timestamp '{TimeStamp}' for device {DeviceId}",
                                        d.TimeStamp,
                                        device.Device);
                                    continue;
                                }

                                allRecords.Add(new PeopleCounter
                                {
                                    DeviceId = device.Device,
                                    Location = d.Location,
                                    SubLocation = d.SubLocation,
                                    InCount = d.Total_IN,
                                    OutCount = d.Total_Out,
                                    Capacity = d.Capacity,
                                    IpAddress = d.ipaddr,
                                    EventTime = ts
                                });
                            }
                        }
                    }
                    catch (JsonException ex)
                    {
                        _logger.LogError(ex, "Failed to deserialize MQTT message");
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to parse individual MQTT message");
                    }
                }

                if (allRecords.Count == 0)
                {
                    _logger.LogDebug("No valid records from {Count} MQTT messages", messages.Count);
                    return;
                }

                var distinctDevices = allRecords
                    .DistinctBy(r => (r.DeviceId, r.Location))
                    .Select(r => new { r.DeviceId, r.Location, r.IpAddress })
                    .ToList();

                foreach (var d in distinctDevices)
                {
                    var sensor = await _sensorCache.EnsureSensorExistsAsync(d.DeviceId, d.Location, d.IpAddress);
                    if (sensor == null)
                        _logger.LogWarning("Could not ensure sensor {DeviceId} exists.", d.DeviceId);
                    else
                        _sensorCache.UpdateStatus(d.DeviceId, SensorStatus.Online, DateTime.Now);
                }

                allRecords.Sort((a, b) => a.EventTime.CompareTo(b.EventTime));

                var newRecords = new List<PeopleCounter>();
                foreach (var record in allRecords)
                {
                    var countKey = (record.DeviceId, record.Location ?? "");
                    if (_lastKnownCounts.TryGetValue(countKey, out var last) &&
                        last.In == record.InCount && last.Out == record.OutCount)
                    {
                        _logger.LogDebug(
                            "Skipping redundant data for {DeviceId} at {Location}: IN={In}, OUT={Out}",
                            record.DeviceId, record.Location, record.InCount, record.OutCount);
                        continue;
                    }

                    newRecords.Add(record);
                    if (_lastKnownCounts.Count >= MAX_LAST_KNOWN_COUNTS)
                    {
                        _logger.LogWarning("_lastKnownCounts hit limit ({Max}), clearing cache.", MAX_LAST_KNOWN_COUNTS);
                        _lastKnownCounts.Clear();
                    }
                    _lastKnownCounts[countKey] = (record.InCount, record.OutCount);
                }

                int skipped = allRecords.Count - newRecords.Count;
                if (skipped > 0)
                    _logger.LogDebug("Skipped {Skipped} redundant records (counts unchanged)", skipped);

                if (newRecords.Count == 0)
                {
                    _logger.LogDebug("All records in batch were redundant, nothing to insert");
                    return;
                }

                _logger.LogDebug(
                    "Parsed {RecordCount} sensor records from {MessageCount} MQTT messages",
                    newRecords.Count,
                    messages.Count);

                using var scope = _scopeFactory.CreateScope();
                var repo = scope.ServiceProvider.GetRequiredService<PeopleCounterRepository>();

                var insertStopwatch = System.Diagnostics.Stopwatch.StartNew();
                var signalRStopwatch = System.Diagnostics.Stopwatch.StartNew();

                await Task.WhenAll(
                    repo.InsertDataAsync(newRecords),
                    SendSignalRUpdates(newRecords, repo)
                );

                insertStopwatch.Stop();
                signalRStopwatch.Stop();

                _logger.LogDebug(
                    "Bulk insert: {Ms}ms for {Count} records",
                    insertStopwatch.ElapsedMilliseconds,
                    newRecords.Count);

                _logger.LogDebug("SignalR updates: {Ms}ms", signalRStopwatch.ElapsedMilliseconds);

                totalStopwatch.Stop();

                _logger.LogDebug(
                    "Batch summary: {MessageCount} msgs → {RecordCount} records ({Skipped} skipped) | " +
                    "Insert: {InsertMs}ms, SignalR: {SignalRMs}ms, Total: {TotalMs}ms",
                    messages.Count,
                    newRecords.Count,
                    skipped,
                    insertStopwatch.ElapsedMilliseconds,
                    signalRStopwatch.ElapsedMilliseconds,
                    totalStopwatch.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Batch processing failed");
            }
        }

        private async Task SendSignalRUpdates(List<PeopleCounter> devices, PeopleCounterRepository repo)
        {
            var tasks = new List<Task>();

            foreach (var device in devices)
            {
                tasks.Add(_hubContext.Clients
                    .Group($"building:{device.Location}")
                    .SendAsync("SensorUpdated", device));
            }

            _logger.LogDebug("Sending {Count} sensor updates", devices.Count);

            var nowTicks = DateTime.UtcNow.Ticks;
            var lastTicks = Interlocked.Read(ref _lastSummaryBroadcastTicks);
            if (nowTicks - lastTicks > SummaryDebounceInterval.Ticks &&
                Interlocked.CompareExchange(ref _lastSummaryBroadcastTicks, nowTicks, lastTicks) == lastTicks)
            {
                tasks.Add(SendBuildingSummaryAsync(repo));
            }

            await Task.WhenAll(tasks);
        }

        private async Task SendBuildingSummaryAsync(PeopleCounterRepository repo)
        {
            try
            {
                var summaries = await repo.GetBuildingSummaryRaw();
                await _hubContext.Clients.Group("dashboard").SendAsync("BuildingSummaryUpdated", summaries);
                _logger.LogDebug("Sent building summary for {Count} buildings.", summaries.Count);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to send building summary.");
            }
        }
    }
}