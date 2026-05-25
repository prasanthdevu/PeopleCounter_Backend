namespace PeopleCounter_Backend.Models
{
    public class ResetLogDto
    {
        public string DeviceId { get; set; } = default!;
        public DateTime ResetTime { get; set; }
        public int ResetInCount { get; set; }
        public int ResetOutCount { get; set; }
    }
}
