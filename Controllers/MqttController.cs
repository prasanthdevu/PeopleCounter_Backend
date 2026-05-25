using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using PeopleCounter_Backend.Data;
using PeopleCounter_Backend.Services;

namespace PeopleCounter_Backend.Controllers
{
    [ApiController]
    [Route("mqtt")]
    public class MqttController : ControllerBase
    {
        private readonly MqttService _mqttService;
        private readonly PeopleCounterRepository _repository;

        public MqttController(MqttService mqttService, PeopleCounterRepository repository)
        {
            _mqttService = mqttService;
            _repository = repository;
        }

        [Authorize(Roles = "Admin")]
        [HttpPost("publish")]
        public async Task<IActionResult> Publish([FromBody] PublishDto dto)
        {
            await _mqttService.Publish(dto.Topic, dto.Payload);
            return Ok("Published");
        }



        [Authorize]
        [HttpGet("buildings")]
        public async Task<IActionResult> GetBuildings()
        {
            var data = await _repository.GetBuildingSummaryRaw();
            return Ok(data);
        }

        [Authorize]
        [HttpGet("building/{building}")]
        public async Task<IActionResult> GetBuildingDevices(string building)
        {
            var data = await _repository.GetSensorsByBuildingRaw(building);
            return Ok(data);
        }


    }

    public record PublishDto(string Topic, string Payload);
}
