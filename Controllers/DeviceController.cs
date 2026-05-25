
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;
using PeopleCounter_Backend.Data;
using PeopleCounter_Backend.Services;

namespace PeopleCounter_Backend.Controllers
{
    [ApiController]
    [Route("device")]
    public class DeviceController : ControllerBase
    {
        private readonly PeopleCounterRepository _repository;
        private readonly IHubContext<PeopleCounterHub> _hub;
        private readonly SensorCacheService _sensorCache;
        private readonly SensorApiService _sensorApi;

        public DeviceController(
            PeopleCounterRepository repository,
            IHubContext<PeopleCounterHub> hub,
            SensorCacheService sensorCache,
            SensorApiService sensorApi)
        {
            _repository = repository;
            _hub = hub;
            _sensorCache = sensorCache;
            _sensorApi = sensorApi;
        }

        [Authorize(Roles = "Admin")]
        [HttpPost("{deviceId}/reset")]
        public async Task<IActionResult> ResetDevice(string deviceId)
        {
            try
            {
                var building = await _repository.GetBuildingByDevice(deviceId);
                await _repository.ResetDevice(deviceId);

                bool hardwareReset = false;
                if (_sensorCache.TryGetSensor(deviceId, out var sensor) && sensor != null)
                    hardwareReset = await _sensorApi.ResetDetectResultAsync(deviceId, sensor.IpAddress);

                await _hub.Clients.Group($"building:{building}").SendAsync("DeviceReset", deviceId);
                var updatedSummaries = await _repository.GetBuildingSummaryRaw();
                await _hub.Clients.Group("dashboard").SendAsync("BuildingSummaryUpdated", updatedSummaries);

                return Ok(new { message = "Device reset successful", deviceId, hardwareReset });
            }
            catch (InvalidOperationException)
            {
                return NotFound(new { error = $"Device '{deviceId}' not found" });
            }
        }

        [Authorize(Roles = "Admin")]
        [HttpPost("building/{building}/reset")]
        public async Task<IActionResult> ResetBuilding(string building)
        {
            await _repository.ResetAllDevicesInBuilding(building);

            var sensors = _sensorCache.GetAll()
                .Where(s => s.Location == building)
                .ToList();

            var hardwareResults = await Task.WhenAll(
                sensors.Select(s => _sensorApi.ResetDetectResultAsync(s.Device, s.IpAddress)));

            var sensorResults = sensors
                .Zip(hardwareResults, (s, ok) => new { s.Device, hardwareReset = ok })
                .ToList();

            await _hub.Clients.Group($"building:{building}").SendAsync("BuildingReset", building);
            var updatedSummaries = await _repository.GetBuildingSummaryRaw();
            await _hub.Clients.Group("dashboard").SendAsync("BuildingSummaryUpdated", updatedSummaries);

            return Ok(new { message = "Building reset successful", building, sensors = sensorResults });
        }


        [HttpGet("resets")]
        public async Task<IActionResult> GetResets()
        {
            var data = await _repository.GetResetsAsync();
            return Ok(data);
        }


        [Authorize]
        [HttpGet("trend")]
        public async Task<IActionResult> GetSensorTrend(
       [FromQuery] string deviceId,
       [FromQuery] DateTime from,
       [FromQuery] DateTime to,
       [FromQuery] string bucket = "hour")
        {
            if (string.IsNullOrWhiteSpace(deviceId))
                return BadRequest("deviceId is required");

            if (from == default || to == default)
                return BadRequest("from and to dates are required");

            if (to < from)
                return BadRequest("to must be greater than from");

            var validBuckets = new[] { "hour", "day", "month" };
            if (!validBuckets.Contains(bucket.ToLower()))
                return BadRequest("bucket must be one of: hour, day, month");

            DateTime adjustedFrom = from;
            DateTime adjustedTo = to;

            switch (bucket.ToLower())
            {
                case "hour":
                    break;

                case "day":
                    adjustedFrom = from.Date;
                    adjustedTo = to.Date.AddDays(1).AddSeconds(-1);
                    break;

                case "month":
                    adjustedFrom = new DateTime(from.Year, from.Month, 1);
                    adjustedTo = new DateTime(to.Year, to.Month, 1).AddMonths(1).AddSeconds(-1);
                    break;
            }

            var data = await _repository.GetSensorTrendRawAsync(deviceId, adjustedFrom, adjustedTo, bucket);
            return Ok(data);
        }

        [Authorize]
        [HttpGet("trendlocation")]
        public async Task<IActionResult> GetLocationTrend(
            [FromQuery] string location,
            [FromQuery] DateTime from,
            [FromQuery] DateTime to,
            [FromQuery] string bucket = "hour")
        {
            if (string.IsNullOrWhiteSpace(location))
                return BadRequest("location is required");

            if (from == default || to == default)
                return BadRequest("from and to dates are required");

            if (to < from)
                return BadRequest("to must be greater than from");

            var validBuckets = new[] { "hour", "day", "month" };
            if (!validBuckets.Contains(bucket.ToLower()))
                return BadRequest("bucket must be one of: hour, day, month");

            DateTime adjustedFrom = from;
            DateTime adjustedTo = to;

            switch (bucket.ToLower())
            {
                case "hour":
                    break;

                case "day":
                    adjustedFrom = from.Date;
                    adjustedTo = to.Date.AddDays(1).AddTicks(-1);
                    break;

                case "month":
                    adjustedFrom = new DateTime(from.Year, from.Month, 1);
                    adjustedTo = new DateTime(to.Year, to.Month, 1).AddMonths(1).AddTicks(-1);
                    break;
            }

            var data = await _repository.GetLocationTrendRawAsync(location, adjustedFrom, adjustedTo, bucket);
            return Ok(data);
        }


        [Authorize]
        [HttpGet("list")]
        public async Task<IActionResult> GetDevices()
            => Ok(await _repository.GetListOfDevices());

        [Authorize]
        [HttpGet("location")]
        public async Task<IActionResult> GetLocation()
            => Ok(await _repository.GetListOfLocation());

        [Authorize]
        [HttpGet("daily-comparison")]
        public async Task<IActionResult> GetDailyComparison(
            [FromQuery] DateOnly date,
            [FromQuery] string? building,
            [FromQuery] string? deviceId)
        {
            if (date == default)
                return BadRequest("date is required");

            if (string.IsNullOrWhiteSpace(building) && string.IsNullOrWhiteSpace(deviceId))
                return BadRequest("Either building or deviceId is required");

            var data = await _repository.GetDailyComparisonAsync(date, building, deviceId);

            if (data.Count == 0)
                return NotFound(new { error = "No data found for the given parameters" });

            return Ok(data);
        }

        [Authorize]
        [HttpGet("status")]
        public async Task<IActionResult> GetSensorStatuses()
        {
            await _sensorCache.InitializeAsync();

            var sensors = _sensorCache.GetAll()
                .Select(s => new
                {
                    s.Device,
                    s.Location,
                    s.IsOnline,
                    s.LastSeen,
                    s.IpAddress,
                    Status = s.Status.ToString()  // "Online", "Idle", "Offline"
                })
                .ToList();

            return Ok(sensors);
        }
    }
}
