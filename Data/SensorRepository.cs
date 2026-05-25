using Microsoft.Data.SqlClient;
using PeopleCounter_Backend.Models;
using System.Net;

namespace PeopleCounter_Backend.Services
{
    public class SensorRepository
    {
        private readonly IConfiguration _config;

        public SensorRepository(IConfiguration config)
        {
            _config = config;
        }

        public async Task<List<Sensor>> GetAllAsync()
        {
            var list = new List<Sensor>();
            using var conn = new SqlConnection(_config.GetConnectionString("DefaultConnection"));
            await conn.OpenAsync();

            var cmd = new SqlCommand(
                "SELECT Id, Device, Location, IpAddress, IsOnline, LastSeen, Status FROM Sensors", conn);

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                list.Add(new Sensor
                {
                    Id       = reader.GetInt32(0),
                    Device   = reader.GetString(1),
                    Location = reader.IsDBNull(2) ? "" : reader.GetString(2),
                    IpAddress = reader.IsDBNull(3) ? "" : reader.GetString(3),
                    IsOnline = reader.GetBoolean(4),
                    LastSeen = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
                    Status   = reader.IsDBNull(6) ? SensorStatus.Offline
                               : Enum.TryParse<SensorStatus>(reader.GetString(6), ignoreCase: true, out var s1) ? s1 : SensorStatus.Offline
                });
            }

            return list;
        }

        public async Task UpdateStatusAsync(int id, SensorStatus status, DateTime? lastSeen)
        {
            using var conn = new SqlConnection(_config.GetConnectionString("DefaultConnection"));
            await conn.OpenAsync();

            using var cmd = new SqlCommand(@"
        UPDATE Sensors
        SET Status   = @status,
            IsOnline = @isOnline,
            LastSeen = ISNULL(@lastSeen, LastSeen)
        WHERE Id = @id", conn);

            cmd.Parameters.AddWithValue("@status", status.ToString());
            cmd.Parameters.AddWithValue("@isOnline", status == SensorStatus.Online);
            cmd.Parameters.AddWithValue("@lastSeen", (object?)lastSeen ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@id", id);

            await cmd.ExecuteNonQueryAsync();
        }

        public async Task<Sensor?> GetByDeviceAsync(string device)
        {
            using var conn = new SqlConnection(_config.GetConnectionString("DefaultConnection"));
            await conn.OpenAsync();

            using var cmd = new SqlCommand(
                @"SELECT Id, Device, Location, IpAddress, IsOnline, LastSeen, Status
          FROM Sensors
          WHERE Device = @device", conn);

            cmd.Parameters.AddWithValue("@device", device);

            using var r = await cmd.ExecuteReaderAsync();
            if (!await r.ReadAsync()) return null;

            return new Sensor
            {
                Id = r.GetInt32(0),
                Device = r.GetString(1),
                Location = r.GetString(2),
                IpAddress = r.IsDBNull(3) ? "" : r.GetString(3),
                IsOnline = r.GetBoolean(4),
                LastSeen = r.IsDBNull(5) ? null : r.GetDateTime(5),
                Status = r.IsDBNull(6) ? SensorStatus.Offline
                         : Enum.TryParse<SensorStatus>(r.GetString(6), ignoreCase: true, out var s2) ? s2 : SensorStatus.Offline
            };
        }
        public async Task InsertIfNotExistsAsync(string device, string location, string ipAddress)
        {
            using var conn = new SqlConnection(_config.GetConnectionString("DefaultConnection"));
            await conn.OpenAsync();

            using var cmd = new SqlCommand(@"
        IF NOT EXISTS (SELECT 1 FROM Sensors WHERE Device = @device)
        INSERT INTO Sensors (Device, Location, IsOnline, IpAddress, LastSeen, Status)
        VALUES (@device, @location, 1, @ip, GETDATE(), 'Online')", conn);

            cmd.Parameters.AddWithValue("@device", device);
            cmd.Parameters.AddWithValue("@location", location);
            cmd.Parameters.AddWithValue("@ip", ipAddress);

            await cmd.ExecuteNonQueryAsync();
        }


        public async Task<bool> IsActiveRecentlyAsync(string device, int minutes = 2)
        {
            using var conn = new SqlConnection(_config.GetConnectionString("DefaultConnection"));
            await conn.OpenAsync();

            using var cmd = new SqlCommand(@"
        SELECT COUNT(1)
        FROM people_counter_log
        WHERE device_id = @device
          AND created_at >= DATEADD(MINUTE, -@minutes, GETDATE())", conn);

            cmd.Parameters.AddWithValue("@device", device);
            cmd.Parameters.AddWithValue("@minutes", minutes);

            var count = (int)await cmd.ExecuteScalarAsync();
            return count > 0;
        }

        public async Task<DateTime?> GetLastDataTimeAsync(string device)
        {
            using var conn = new SqlConnection(_config.GetConnectionString("DefaultConnection"));
            await conn.OpenAsync();

            using var cmd = new SqlCommand(@"
        SELECT TOP 1 created_at
        FROM people_counter_log
        WHERE device_id = @device
        ORDER BY created_at DESC", conn);

            cmd.Parameters.AddWithValue("@device", device);

            var result = await cmd.ExecuteScalarAsync();
            return result == null || result == DBNull.Value
                ? null
                : (DateTime)result;
        }
    }
}
