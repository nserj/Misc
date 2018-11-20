using DP2SaaSMCS.Tasks;
using Microsoft.AspNetCore.Mvc;
using ScheduledService.Code;
using System.Collections.Generic;
using System.Linq;

namespace DP2SaaSMCS.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class StateController : ControllerBase
    {

        private CurrentTask sch_task;

        public StateController(IEnumerable<IScheduledTask> scheduledTasks)
        {
            sch_task = (CurrentTask) scheduledTasks.FirstOrDefault();
        }

        // GET api/values
        [HttpGet]
        [Route("GetState")]
        public ActionResult<TaskStateReport> GetState()
        {
            return (TaskStateReport) sch_task.State.ToReport();
        }

        [HttpPost]
        [Route("Freeze")]
        public ActionResult<int> Freeze([FromBody] bool state)
        {
            sch_task.State.Freezed = state;

            return Ok(1);
        }

    }
}
