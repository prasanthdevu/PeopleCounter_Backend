using Microsoft.Extensions.Options;
using PeopleCounter_Backend.Models;
using System.Net;

namespace PeopleCounter_Backend.Services
{
    public class SensorApiService
    {
        private readonly SensorApiOptions _options;
        private readonly ILogger<SensorApiService> _logger;

        public SensorApiService(IOptions<SensorApiOptions> options, ILogger<SensorApiService> logger)
        {
            _options = options.Value;
            _logger = logger;
        }

        public async Task<bool> ResetDetectResultAsync(string deviceId, string ip)
        {
            if (string.IsNullOrWhiteSpace(ip))
            {
                _logger.LogWarning("Sensor {DeviceId} has no IP address — skipping hardware reset", deviceId);
                return false;
            }

            try
            {
                var uri = new Uri($"https://{ip}");
                var credentials = new CredentialCache
        {
            { uri, "Digest", new NetworkCredential(_options.Username, _options.Password) }
        };

                var handler = new HttpClientHandler
                {
                    Credentials = credentials,
                    ServerCertificateCustomValidationCallback =
                        HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
                };

                using var client = new HttpClient(handler)
                {
                    Timeout = TimeSpan.FromSeconds(_options.TimeoutSeconds)
                };

                var jsonContent = new StringContent(
                    """{"lineId":[0],"regionId":[],"gazeId":[]}""",
                    System.Text.Encoding.UTF8,
                    "application/json");

                var response = await client.PostAsync(
                    $"https://{ip}/api/v1/system/resetDetectResult",
                    jsonContent);

                if (response.IsSuccessStatusCode)
                {
                    _logger.LogInformation(
                        "Hardware reset succeeded for sensor {DeviceId} ({Ip})", deviceId, ip);
                    return true;
                }

                _logger.LogWarning(
                    "Hardware reset failed for sensor {DeviceId} ({Ip}) — HTTP {Status}",
                    deviceId, ip, (int)response.StatusCode);
                return false;
            }
            catch (TaskCanceledException)
            {
                _logger.LogWarning(
                    "Hardware reset timed out for sensor {DeviceId} ({Ip})", deviceId, ip);
                return false;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Hardware reset error for sensor {DeviceId} ({Ip})", deviceId, ip);
                return false;
            }
        }
    }
}
