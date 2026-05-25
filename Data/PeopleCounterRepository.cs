



using Microsoft.Data.SqlClient;
using PeopleCounter_Backend.Models;
using System.Data;
using System.Net.NetworkInformation;

namespace PeopleCounter_Backend.Data
{
    public class PeopleCounterRepository
    {
        private readonly string _connectionString;

        public PeopleCounterRepository(IConfiguration configuration)
        {
            _connectionString = configuration.GetConnectionString("DefaultConnection");
        }


        //Get all sensors
        public async Task<List<string>> GetListOfDevices()
        {
            const string sql = @"
                SELECT Device
                FROM Sensors
                ORDER BY Device";

            var list = new List<string>();

            using var conn = new SqlConnection(_connectionString);
            using var cmd = new SqlCommand(sql, conn);

            await conn.OpenAsync();
            using var r = await cmd.ExecuteReaderAsync();

            while (await r.ReadAsync())
                list.Add(r.GetString(0));

            return list;
        }

        //Get all locations
        public async Task<List<string>> GetListOfLocation()
        {
            const string sql = @"
                SELECT DISTINCT Location
                FROM Sensors
                ORDER BY Location";

            var list = new List<string>();

            using var conn = new SqlConnection(_connectionString);
            using var cmd = new SqlCommand(sql, conn);

            await conn.OpenAsync();
            using var r = await cmd.ExecuteReaderAsync();

            while (await r.ReadAsync())
                list.Add(r.GetString(0));

            return list;
        }

        //Reset sensor
        public async Task ResetDevice(string deviceId)
        {

            var sqlGetLatestRecord = @"
                SELECT TOP 1
                    in_count,
                    out_count,
                    event_time
                FROM dbo.people_counter_log
                WHERE device_id = @deviceId
                ORDER BY event_time DESC, id DESC;";

            var sqlInsertReset = @"
                INSERT INTO dbo.people_counter_resets
                (device_id, reset_time, reset_in_count, reset_out_count)
                VALUES
                (@deviceId, GETDATE(), @resetIn, @resetOut);";

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            int inCount;
            int outCount;

            using (var cmd = new SqlCommand(sqlGetLatestRecord, conn))
            {
                cmd.Parameters.AddWithValue("@deviceId", deviceId);

                using var reader = await cmd.ExecuteReaderAsync();
                if (!await reader.ReadAsync())
                    throw new InvalidOperationException("No data found for device");

                inCount = reader.GetInt32(0);
                outCount = reader.GetInt32(1);
            }

            using (var cmd = new SqlCommand(sqlInsertReset, conn))
            {
                cmd.Parameters.AddWithValue("@deviceId", deviceId);
                cmd.Parameters.AddWithValue("@resetIn", inCount);
                cmd.Parameters.AddWithValue("@resetOut", outCount);
                await cmd.ExecuteNonQueryAsync();
            }
        }

        //Reset Location — single batched INSERT, replaces N+1 per-device loop
        public async Task ResetAllDevicesInBuilding(string building)
        {
            const string sql = @"
                INSERT INTO dbo.people_counter_resets (device_id, reset_time, reset_in_count, reset_out_count)
                SELECT latest.device_id, GETDATE(), latest.in_count, latest.out_count
                FROM (
                    SELECT device_id, in_count, out_count,
                           ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY event_time DESC, id DESC) AS rn
                    FROM dbo.people_counter_log
                    WHERE location = @building
                ) latest
                WHERE latest.rn = 1;";

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@building", building);
            await cmd.ExecuteNonQueryAsync();
        }

        //Insert 
        public async Task InsertDataAsync(IEnumerable<PeopleCounter> records)
        {
            var recordList = records.ToList();
            if (recordList.Count == 0)
            {
                return;
            }

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var dataTable = new DataTable();

            dataTable.Columns.Add("device_id", typeof(string));
            dataTable.Columns.Add("location", typeof(string));
            dataTable.Columns.Add("sublocation", typeof(string));
            dataTable.Columns.Add("in_count", typeof(int));
            dataTable.Columns.Add("out_count", typeof(int));
            dataTable.Columns.Add("capacity", typeof(int));
            dataTable.Columns.Add("event_time", typeof(DateTime));


            foreach (var item in recordList)
            {
                dataTable.Rows.Add(
                    item.DeviceId, item.Location ?? (object)DBNull.Value, item.SubLocation ?? (object)DBNull.Value,
                    item.InCount, item.OutCount, item.Capacity, item.EventTime);
            }
            using var bulkCopy = new SqlBulkCopy(conn)
            {
                DestinationTableName = "people_counter_log",
                BatchSize = 1000,
                BulkCopyTimeout = 60,
                EnableStreaming = true
            };

            bulkCopy.ColumnMappings.Add("device_id", "device_id");
            bulkCopy.ColumnMappings.Add("location", "location");
            bulkCopy.ColumnMappings.Add("sublocation", "sublocation");
            bulkCopy.ColumnMappings.Add("in_count", "in_count");
            bulkCopy.ColumnMappings.Add("out_count", "out_count");
            bulkCopy.ColumnMappings.Add("capacity", "capacity");
            bulkCopy.ColumnMappings.Add("event_time", "event_time");

            await bulkCopy.WriteToServerAsync(dataTable);
        }

        //Get Location by device
        public async Task<string> GetBuildingByDevice(string deviceId)
        {
            const string sql = @"
        SELECT TOP 1 location
        FROM people_counter_log
        WHERE device_id = @deviceId
        ORDER BY created_at DESC, id DESC;
    ";

            using var conn = new SqlConnection(_connectionString);
            using var cmd = new SqlCommand(sql, conn);

            cmd.Parameters.AddWithValue("@deviceId", deviceId);

            await conn.OpenAsync();

            var result = await cmd.ExecuteScalarAsync();

            if (result == null || result == DBNull.Value)
                throw new InvalidOperationException(
                    $"No building found for device '{deviceId}'");

            return (string)result;
        }

        //Get Buildings Summary
        public async Task<List<BuildingSummary>> GetBuildingSummary()
        {
            const string sql = @"
    WITH latest_reset AS (
        SELECT
            device_id,
            reset_in_count,
            reset_out_count,
            ROW_NUMBER() OVER (
                PARTITION BY device_id
                ORDER BY reset_time DESC
            ) AS rn
        FROM people_counter_resets
    ),
    latest_log AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY device_id, location
                   ORDER BY created_at DESC, id DESC
               ) AS rn
        FROM people_counter_log
        WHERE created_at >= DATEADD(day, -30, GETDATE())
    ),
    calculated AS (
        SELECT
            l.location,
            CASE
                WHEN r.reset_in_count IS NULL        THEN l.in_count
                WHEN l.in_count < r.reset_in_count   THEN l.in_count
                ELSE l.in_count - r.reset_in_count
            END AS display_in,
            CASE
                WHEN r.reset_out_count IS NULL       THEN l.out_count
                WHEN l.out_count < r.reset_out_count THEN l.out_count
                ELSE l.out_count - r.reset_out_count
            END AS display_out
        FROM latest_log l
        LEFT JOIN latest_reset r
            ON l.device_id = r.device_id
            AND r.rn = 1
        WHERE l.rn = 1
    )
    SELECT
        location                            AS Building,
        SUM(display_in)                     AS TotalIn,
        SUM(display_out)                    AS TotalOut,
        SUM(display_in) - SUM(display_out) AS TotalCapacity
    FROM calculated
    GROUP BY location;";

            var result = new List<BuildingSummary>();

            using var conn = new SqlConnection(_connectionString);
            using var cmd = new SqlCommand(sql, conn);

            await conn.OpenAsync();
            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                result.Add(new BuildingSummary(
                    Building: reader.GetString(0),
                    TotalIn: reader.GetInt32(1),
                    TotalOut: reader.GetInt32(2),
                    TotalCapacity: reader.GetInt32(3)
                ));
            }

            return result;
        }

        //Get all sensor details in building
        public async Task<List<PeopleCounter>> GetSensorsByBuilding(string building)
        {
            const string sql = @"
    WITH latest_reset AS (
        SELECT
            device_id,
            reset_in_count,
            reset_out_count,
            ROW_NUMBER() OVER (
                PARTITION BY device_id
                ORDER BY reset_time DESC
            ) AS rn
        FROM people_counter_resets
    ),
    latest_log AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY device_id, location
                   ORDER BY created_at DESC, id DESC
               ) AS rn
        FROM people_counter_log
        WHERE location = @building
          AND created_at >= DATEADD(day, -30, GETDATE())
    ),
    calculated AS (
        SELECT
            l.device_id,
            l.location,
            l.sublocation,
            l.event_time,
            CASE
                WHEN r.reset_in_count IS NULL        THEN l.in_count
                WHEN l.in_count < r.reset_in_count   THEN l.in_count
                ELSE l.in_count - r.reset_in_count
            END AS display_in,
            CASE
                WHEN r.reset_out_count IS NULL       THEN l.out_count
                WHEN l.out_count < r.reset_out_count THEN l.out_count
                ELSE l.out_count - r.reset_out_count
            END AS display_out
        FROM latest_log l
        LEFT JOIN latest_reset r
            ON l.device_id = r.device_id
            AND r.rn = 1
        WHERE l.rn = 1
    )
    SELECT
        device_id,
        location,
        sublocation,
        event_time,
        display_in,
        display_out,
        display_in - display_out AS inside
    FROM calculated;";

            var result = new List<PeopleCounter>();

            using var conn = new SqlConnection(_connectionString);
            using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@building", building);

            await conn.OpenAsync();
            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                result.Add(new PeopleCounter
                {
                    DeviceId = reader.GetString(0),
                    Location = reader.GetString(1),
                    SubLocation = reader.IsDBNull(2) ? null : reader.GetString(2),
                    EventTime = reader.GetDateTime(3),
                    InCount = reader.GetInt32(4),
                    OutCount = reader.GetInt32(5),
                    Capacity = reader.GetInt32(6)
                });
            }

            return result;
        }

        //Get Buildings Summary Wihtout Reset-Aware
        public async Task<List<BuildingSummary>> GetBuildingSummaryRaw()
        {
            const string sql = @"
    WITH latest_log AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY device_id, location
                   ORDER BY created_at DESC, id DESC
               ) AS rn
        FROM people_counter_log
        WHERE created_at >= DATEADD(day, -30, GETDATE())
    )
    SELECT
        location                          AS Building,
        SUM(in_count)                     AS TotalIn,
        SUM(out_count)                    AS TotalOut,
        SUM(in_count) - SUM(out_count)    AS TotalCapacity
    FROM latest_log
    WHERE rn = 1
    GROUP BY location;";

            var result = new List<BuildingSummary>();

            using var conn = new SqlConnection(_connectionString);
            using var cmd = new SqlCommand(sql, conn);

            await conn.OpenAsync();
            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                result.Add(new BuildingSummary(
                    Building: reader.GetString(0),
                    TotalIn: reader.GetInt32(1),
                    TotalOut: reader.GetInt32(2),
                    TotalCapacity: reader.GetInt32(3)
                ));
            }

            return result;
        }

        //Get all sensor details in building Wihtout Reset-Aware
        public async Task<List<PeopleCounter>> GetSensorsByBuildingRaw(string building)
        {
            const string sql = @"
    WITH latest_log AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY device_id, location
                   ORDER BY created_at DESC, id DESC
               ) AS rn
        FROM people_counter_log
        WHERE location = @building
          AND created_at >= DATEADD(day, -30, GETDATE())
    )
    SELECT
        device_id,
        location,
        sublocation,
        event_time,
        in_count,
        out_count,
        in_count - out_count AS inside
    FROM latest_log
    WHERE rn = 1;";

            var result = new List<PeopleCounter>();

            using var conn = new SqlConnection(_connectionString);
            using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@building", building);

            await conn.OpenAsync();
            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                result.Add(new PeopleCounter
                {
                    DeviceId = reader.GetString(0),
                    Location = reader.GetString(1),
                    SubLocation = reader.IsDBNull(2) ? null : reader.GetString(2),
                    EventTime = reader.GetDateTime(3),
                    InCount = reader.GetInt32(4),
                    OutCount = reader.GetInt32(5),
                    Capacity = reader.GetInt32(6)
                });
            }

            return result;
        }

        //sending update for device
        public async Task<List<PeopleCounter>> GetLatestLogicalDeviceByIdsAysnc(List<string> deviceIds)
        {
            if (deviceIds == null || deviceIds.Count == 0)
                return new List<PeopleCounter>();

            const string sql = @"
    WITH latest_reset AS (
        SELECT
            device_id,
            reset_in_count,
            reset_out_count,
            ROW_NUMBER() OVER (
                PARTITION BY device_id
                ORDER BY reset_time DESC
            ) AS rn
        FROM people_counter_resets
        WHERE device_id IN ({0})
    ),
    latest_log AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY device_id, location
                   ORDER BY created_at DESC, id DESC
               ) AS rn
        FROM people_counter_log
        WHERE device_id IN ({0})
          AND created_at >= DATEADD(day, -30, GETDATE())
    ),
    calculated AS (
        SELECT
            l.device_id,
            l.location,
            l.sublocation,
            l.event_time,
            CASE
                WHEN r.reset_in_count IS NULL        THEN l.in_count
                WHEN l.in_count < r.reset_in_count   THEN l.in_count
                ELSE l.in_count - r.reset_in_count
            END AS display_in,
            CASE
                WHEN r.reset_out_count IS NULL       THEN l.out_count
                WHEN l.out_count < r.reset_out_count THEN l.out_count
                ELSE l.out_count - r.reset_out_count
            END AS display_out
        FROM latest_log l
        LEFT JOIN latest_reset r
            ON l.device_id = r.device_id
            AND r.rn = 1
        WHERE l.rn = 1
    )
    SELECT
        device_id,
        location,
        sublocation,
        event_time,
        display_in,
        display_out,
        display_in - display_out AS inside
    FROM calculated;";

            var parameters = deviceIds.Select((_, i) => $"@DeviceId{i}").ToList();
            var finalSql = string.Format(sql, string.Join(",", parameters));

            var result = new List<PeopleCounter>();
            using var conn = new SqlConnection(_connectionString);
            using var cmd = new SqlCommand(finalSql, conn);

            for (int i = 0; i < deviceIds.Count; i++)
                cmd.Parameters.AddWithValue($"@DeviceId{i}", deviceIds[i]);

            await conn.OpenAsync();
            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                result.Add(new PeopleCounter
                {
                    DeviceId = reader.GetString(0),
                    Location = reader.GetString(1),
                    SubLocation = reader.IsDBNull(2) ? null : reader.GetString(2),
                    EventTime = reader.GetDateTime(3),
                    InCount = reader.GetInt32(4),
                    OutCount = reader.GetInt32(5),
                    Capacity = reader.GetInt32(6)
                });
            }

            return result;
        }

        //Sensor Chart
        public async Task<List<SensorTrendPointDto>> GetSensorTrendAsync(string deviceId, DateTime from, DateTime to, string bucket)
        {
            const string sql = @"WITH reset_adjusted AS (
    SELECT
        l.event_time,
        CASE
            WHEN r.reset_in_count IS NULL        THEN l.in_count
            WHEN l.in_count < r.reset_in_count   THEN l.in_count
            ELSE l.in_count - r.reset_in_count
        END AS adj_in,
        CASE
            WHEN r.reset_out_count IS NULL        THEN l.out_count
            WHEN l.out_count < r.reset_out_count  THEN l.out_count
            ELSE l.out_count - r.reset_out_count
        END AS adj_out
    FROM (
        SELECT device_id, event_time, in_count, out_count FROM people_counter_log
        UNION ALL
        SELECT device_id, event_time, in_count, out_count FROM people_counter_log_archive
    ) l
    OUTER APPLY (
        SELECT TOP 1 reset_in_count, reset_out_count
        FROM people_counter_resets r
        WHERE r.device_id = l.device_id
          AND r.reset_time <= l.event_time
        ORDER BY r.reset_time DESC
    ) r
    WHERE l.device_id = @deviceId
      AND l.event_time BETWEEN @fromDate AND @toDate
),
bucketed AS (
    SELECT
        CASE
            WHEN @bucket = 'hour'  THEN DATEADD(hour, DATEDIFF(hour, 0, event_time), 0)
            WHEN @bucket = 'day'   THEN CAST(CAST(event_time AS DATE) AS DATETIME)
            WHEN @bucket = 'month' THEN CAST(DATEFROMPARTS(YEAR(event_time), MONTH(event_time), 1) AS DATETIME)
        END AS bucket_time,
        MAX(adj_in)  AS cum_in,
        MAX(adj_out) AS cum_out,
        MIN(adj_in)  AS first_in,
        MIN(adj_out) AS first_out
    FROM reset_adjusted
    GROUP BY
        CASE
            WHEN @bucket = 'hour'  THEN DATEADD(hour, DATEDIFF(hour, 0, event_time), 0)
            WHEN @bucket = 'day'   THEN CAST(CAST(event_time AS DATE) AS DATETIME)
            WHEN @bucket = 'month' THEN CAST(DATEFROMPARTS(YEAR(event_time), MONTH(event_time), 1) AS DATETIME)
        END
),
diffs AS (
    SELECT
        bucket_time,
        CASE
            WHEN LAG(cum_in)  OVER (ORDER BY bucket_time) IS NULL
            THEN cum_in - first_in      -- ✅ first bucket: MAX - MIN of that bucket
            ELSE cum_in  - LAG(cum_in)  OVER (ORDER BY bucket_time)
        END AS bucket_in,
        CASE
            WHEN LAG(cum_out) OVER (ORDER BY bucket_time) IS NULL
            THEN cum_out - first_out    -- ✅ first bucket: MAX - MIN of that bucket
            ELSE cum_out - LAG(cum_out) OVER (ORDER BY bucket_time)
        END AS bucket_out
    FROM bucketed
)
SELECT
    bucket_time AS [time],
    CASE WHEN bucket_in  < 0 THEN 0 ELSE bucket_in  END AS [in],
    CASE WHEN bucket_out < 0 THEN 0 ELSE bucket_out END AS [out]
FROM diffs
ORDER BY bucket_time;";

            var result = new List<SensorTrendPointDto>();

            using var conn = new SqlConnection(_connectionString);
            using var cmd = new SqlCommand(sql, conn);

            cmd.Parameters.AddWithValue("@deviceId", deviceId);
            cmd.Parameters.AddWithValue("@fromDate", from);
            cmd.Parameters.AddWithValue("@toDate", to);
            cmd.Parameters.AddWithValue("@bucket", bucket);

            await conn.OpenAsync();
            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                result.Add(new SensorTrendPointDto
                {
                    Time = reader.GetDateTime(0),
                    In = Convert.ToInt32(reader.GetValue(1)),
                    Out = Convert.ToInt32(reader.GetValue(2))
                });
            }

            return result;
        }

        //Location chart
        public async Task<List<SensorTrendPointDto>> GetLocationTrendAsync(string location, DateTime from, DateTime to, string bucket)
        {
            const string sql = @"WITH reset_adjusted AS (
    SELECT
        l.device_id,
        l.event_time,
        CASE
            WHEN r.reset_in_count IS NULL           THEN l.in_count
            WHEN l.in_count < r.reset_in_count      THEN l.in_count
            ELSE l.in_count - r.reset_in_count
        END AS adj_in,
        CASE
            WHEN r.reset_out_count IS NULL          THEN l.out_count
            WHEN l.out_count < r.reset_out_count    THEN l.out_count
            ELSE l.out_count - r.reset_out_count
        END AS adj_out
    FROM (
        SELECT device_id, location, event_time, in_count, out_count FROM people_counter_log
        UNION ALL
        SELECT device_id, location, event_time, in_count, out_count FROM people_counter_log_archive
    ) l
    OUTER APPLY (
        SELECT TOP 1 reset_in_count, reset_out_count
        FROM people_counter_resets r
        WHERE r.device_id = l.device_id
          AND r.reset_time <= l.event_time
        ORDER BY r.reset_time DESC
    ) r
    WHERE l.location = @location
      AND l.event_time BETWEEN @fromDate AND @toDate
),
bucketed AS (
    SELECT
        device_id,
        CASE
            WHEN @bucket = 'hour'  THEN DATEADD(hour, DATEDIFF(hour, 0, event_time), 0)
            WHEN @bucket = 'day'   THEN CAST(CAST(event_time AS DATE) AS DATETIME)
            WHEN @bucket = 'month' THEN CAST(DATEFROMPARTS(YEAR(event_time), MONTH(event_time), 1) AS DATETIME)
        END AS bucket_time,
        MAX(adj_in)  AS cum_in,
        MAX(adj_out) AS cum_out,
        MIN(adj_in)  AS min_in,   -- ✅ first value in bucket
        MIN(adj_out) AS min_out
    FROM reset_adjusted
    GROUP BY
        device_id,
        CASE
            WHEN @bucket = 'hour'  THEN DATEADD(hour, DATEDIFF(hour, 0, event_time), 0)
            WHEN @bucket = 'day'   THEN CAST(CAST(event_time AS DATE) AS DATETIME)
            WHEN @bucket = 'month' THEN CAST(DATEFROMPARTS(YEAR(event_time), MONTH(event_time), 1) AS DATETIME)
        END
),
diffs AS (
    SELECT
        device_id,
        bucket_time,
        -- ✅ If first bucket (no previous row), delta = MAX - MIN of that bucket
        -- If subsequent bucket, delta = this MAX - previous MAX
        CASE
            WHEN LAG(cum_in) OVER (PARTITION BY device_id ORDER BY bucket_time) IS NULL
            THEN cum_in - min_in
            ELSE cum_in - LAG(cum_in) OVER (PARTITION BY device_id ORDER BY bucket_time)
        END AS bucket_in,
        CASE
            WHEN LAG(cum_out) OVER (PARTITION BY device_id ORDER BY bucket_time) IS NULL
            THEN cum_out - min_out
            ELSE cum_out - LAG(cum_out) OVER (PARTITION BY device_id ORDER BY bucket_time)
        END AS bucket_out
    FROM bucketed
)
SELECT
    bucket_time                                                  AS [time],
    SUM(CASE WHEN bucket_in  < 0 THEN 0 ELSE bucket_in  END)    AS total_in,
    SUM(CASE WHEN bucket_out < 0 THEN 0 ELSE bucket_out END)    AS total_out
FROM diffs
GROUP BY bucket_time
ORDER BY bucket_time;";

            var result = new List<SensorTrendPointDto>();

            using var conn = new SqlConnection(_connectionString);
            using var cmd = new SqlCommand(sql, conn);

            cmd.Parameters.AddWithValue("@location", location);
            cmd.Parameters.AddWithValue("@fromDate", from);
            cmd.Parameters.AddWithValue("@toDate", to);
            cmd.Parameters.AddWithValue("@bucket", bucket);

            await conn.OpenAsync();
            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                result.Add(new SensorTrendPointDto
                {
                    Time = reader.GetDateTime(0),
                    In = Convert.ToInt32(reader.GetValue(1)),
                    Out = Convert.ToInt32(reader.GetValue(2))
                });
            }

            return result;
        }

        //Sensor Chart Raw
        public async Task<List<SensorTrendPointDto>> GetSensorTrendRawAsync(string deviceId, DateTime from, DateTime to, string bucket)
        {
            const string sql = @"WITH bucketed AS (
    SELECT
        CASE
            WHEN @bucket = 'hour'  THEN DATEADD(hour, DATEDIFF(hour, 0, event_time), 0)
            WHEN @bucket = 'day'   THEN CAST(CAST(event_time AS DATE) AS DATETIME)
            WHEN @bucket = 'month' THEN CAST(DATEFROMPARTS(YEAR(event_time), MONTH(event_time), 1) AS DATETIME)
        END AS bucket_time,
        MAX(in_count)  AS cum_in,
        MAX(out_count) AS cum_out,
        MIN(in_count)  AS first_in,
        MIN(out_count) AS first_out
    FROM (
        SELECT device_id, event_time, in_count, out_count FROM people_counter_log
        UNION ALL
        SELECT device_id, event_time, in_count, out_count FROM people_counter_log_archive
    ) l
    WHERE device_id = @deviceId
      AND event_time BETWEEN @fromDate AND @toDate
    GROUP BY
        CASE
            WHEN @bucket = 'hour'  THEN DATEADD(hour, DATEDIFF(hour, 0, event_time), 0)
            WHEN @bucket = 'day'   THEN CAST(CAST(event_time AS DATE) AS DATETIME)
            WHEN @bucket = 'month' THEN CAST(DATEFROMPARTS(YEAR(event_time), MONTH(event_time), 1) AS DATETIME)
        END
),
diffs AS (
    SELECT
        bucket_time,
        CASE
            WHEN LAG(cum_in)  OVER (ORDER BY bucket_time) IS NULL
            THEN cum_in - first_in
            ELSE cum_in  - LAG(cum_in)  OVER (ORDER BY bucket_time)
        END AS bucket_in,
        CASE
            WHEN LAG(cum_out) OVER (ORDER BY bucket_time) IS NULL
            THEN cum_out - first_out
            ELSE cum_out - LAG(cum_out) OVER (ORDER BY bucket_time)
        END AS bucket_out
    FROM bucketed
)
SELECT
    bucket_time AS [time],
    CASE WHEN bucket_in  < 0 THEN 0 ELSE bucket_in  END AS [in],
    CASE WHEN bucket_out < 0 THEN 0 ELSE bucket_out END AS [out]
FROM diffs
ORDER BY bucket_time;";

            var result = new List<SensorTrendPointDto>();

            using var conn = new SqlConnection(_connectionString);
            using var cmd = new SqlCommand(sql, conn);

            cmd.Parameters.AddWithValue("@deviceId", deviceId);
            cmd.Parameters.AddWithValue("@fromDate", from);
            cmd.Parameters.AddWithValue("@toDate", to);
            cmd.Parameters.AddWithValue("@bucket", bucket);

            await conn.OpenAsync();
            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                result.Add(new SensorTrendPointDto
                {
                    Time = reader.GetDateTime(0),
                    In = Convert.ToInt32(reader.GetValue(1)),
                    Out = Convert.ToInt32(reader.GetValue(2))
                });
            }

            return result;
        }

        //Location Chart Raw
        public async Task<List<SensorTrendPointDto>> GetLocationTrendRawAsync(string location, DateTime from, DateTime to, string bucket)
        {
            const string sql = @"WITH bucketed AS (
    SELECT
        device_id,
        CASE
            WHEN @bucket = 'hour'  THEN DATEADD(hour, DATEDIFF(hour, 0, event_time), 0)
            WHEN @bucket = 'day'   THEN CAST(CAST(event_time AS DATE) AS DATETIME)
            WHEN @bucket = 'month' THEN CAST(DATEFROMPARTS(YEAR(event_time), MONTH(event_time), 1) AS DATETIME)
        END AS bucket_time,
        MAX(in_count)  AS cum_in,
        MAX(out_count) AS cum_out,
        MIN(in_count)  AS min_in,
        MIN(out_count) AS min_out
    FROM (
        SELECT device_id, location, event_time, in_count, out_count FROM people_counter_log
        UNION ALL
        SELECT device_id, location, event_time, in_count, out_count FROM people_counter_log_archive
    ) l
    WHERE location = @location
      AND event_time BETWEEN @fromDate AND @toDate
    GROUP BY
        device_id,
        CASE
            WHEN @bucket = 'hour'  THEN DATEADD(hour, DATEDIFF(hour, 0, event_time), 0)
            WHEN @bucket = 'day'   THEN CAST(CAST(event_time AS DATE) AS DATETIME)
            WHEN @bucket = 'month' THEN CAST(DATEFROMPARTS(YEAR(event_time), MONTH(event_time), 1) AS DATETIME)
        END
),
diffs AS (
    SELECT
        device_id,
        bucket_time,
        CASE
            WHEN LAG(cum_in) OVER (PARTITION BY device_id ORDER BY bucket_time) IS NULL
            THEN cum_in - min_in
            ELSE cum_in - LAG(cum_in) OVER (PARTITION BY device_id ORDER BY bucket_time)
        END AS bucket_in,
        CASE
            WHEN LAG(cum_out) OVER (PARTITION BY device_id ORDER BY bucket_time) IS NULL
            THEN cum_out - min_out
            ELSE cum_out - LAG(cum_out) OVER (PARTITION BY device_id ORDER BY bucket_time)
        END AS bucket_out
    FROM bucketed
)
SELECT
    bucket_time                                                  AS [time],
    SUM(CASE WHEN bucket_in  < 0 THEN 0 ELSE bucket_in  END)    AS total_in,
    SUM(CASE WHEN bucket_out < 0 THEN 0 ELSE bucket_out END)    AS total_out
FROM diffs
GROUP BY bucket_time
ORDER BY bucket_time;";

            var result = new List<SensorTrendPointDto>();

            using var conn = new SqlConnection(_connectionString);
            using var cmd = new SqlCommand(sql, conn);

            cmd.Parameters.AddWithValue("@location", location);
            cmd.Parameters.AddWithValue("@fromDate", from);
            cmd.Parameters.AddWithValue("@toDate", to);
            cmd.Parameters.AddWithValue("@bucket", bucket);

            await conn.OpenAsync();
            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                result.Add(new SensorTrendPointDto
                {
                    Time = reader.GetDateTime(0),
                    In = Convert.ToInt32(reader.GetValue(1)),
                    Out = Convert.ToInt32(reader.GetValue(2))
                });
            }

            return result;
        }

        public async Task<List<ResetLogDto>> GetResetsAsync()
        {
            var sql = @"
                SELECT r.device_id, r.reset_time, r.reset_in_count, r.reset_out_count
                FROM dbo.people_counter_resets r";

            var result = new List<ResetLogDto>();

            using var conn = new SqlConnection(_connectionString);
            using var cmd = new SqlCommand(sql, conn);

            await conn.OpenAsync();
            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                result.Add(new ResetLogDto
                {
                    DeviceId      = reader.GetString(0),
                    ResetTime     = reader.GetDateTime(1),
                    ResetInCount  = reader.GetInt32(2),
                    ResetOutCount = reader.GetInt32(3)
                });
            }

            return result;
        }

        //Comaparison
        public async Task<List<DailyComparisonDto>> GetDailyComparisonAsync(DateOnly date, string? building,string? deviceId)
        {
            const string sql = @"WITH hourly_max AS (
    SELECT
        device_id,
        location,
        sublocation,
        DATEADD(MINUTE, DATEDIFF(MINUTE, 0, event_time), 0) AS hour_bucket,
        MAX(in_count)  AS raw_in,
        MAX(out_count) AS raw_out
    FROM (
        SELECT device_id, location, sublocation, event_time, in_count, out_count FROM people_counter_log
        UNION ALL
        SELECT device_id, location, sublocation, event_time, in_count, out_count FROM people_counter_log_archive
    ) AS combined
    WHERE CAST(event_time AS DATE) = @date
      AND (@building IS NULL OR location  = @building)
      AND (@deviceId IS NULL OR device_id = @deviceId)
    GROUP BY
        device_id,
        location,
        sublocation,
        DATEADD(MINUTE, DATEDIFF(MINUTE, 0, event_time), 0)
)
SELECT
    h.device_id,
    h.location,
    h.sublocation,
    h.hour_bucket,
    h.raw_in,
    h.raw_out,
    CASE WHEN h.raw_in >= h.raw_out THEN h.raw_in - h.raw_out ELSE 0 END AS raw_inside
FROM hourly_max h
ORDER BY h.device_id, h.hour_bucket;";

            var result = new List<DailyComparisonDto>();

            using var conn = new SqlConnection(_connectionString);
            using var cmd = new SqlCommand(sql, conn);

            cmd.Parameters.AddWithValue("@date", date.ToDateTime(TimeOnly.MinValue));
            cmd.Parameters.AddWithValue("@building", (object?)building ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@deviceId", (object?)deviceId ?? DBNull.Value);

            await conn.OpenAsync();
            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                var displayIn  = reader.GetInt32(7);
                var displayOut = reader.GetInt32(8);

                result.Add(new DailyComparisonDto
                {
                    DeviceId       = reader.GetString(0),
                    Location       = reader.GetString(1),
                    SubLocation    = reader.IsDBNull(2) ? null : reader.GetString(2),
                    Hour           = reader.GetDateTime(3),
                    RawIn          = reader.GetInt32(4),
                    RawOut         = reader.GetInt32(5),
                    RawInside      = reader.GetInt32(6),
                    DisplayIn      = displayIn,
                    DisplayOut     = displayOut,
                    DisplayInside  = displayIn - displayOut
                });
            }
            return result;
        }
    }
}
