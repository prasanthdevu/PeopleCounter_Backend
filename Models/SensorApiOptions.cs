namespace PeopleCounter_Backend.Models
{
    public class SensorApiOptions
    {
        public string Username { get; set; } = default!;
        public string Password { get; set; } = default!;
        public int TimeoutSeconds { get; set; } = 10;
    }
}
