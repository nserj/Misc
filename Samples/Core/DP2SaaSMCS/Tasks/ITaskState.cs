using ScheduledService.Code;

namespace DP2SaaSMCS.Tasks
{
    public class ITaskState : IScheduledTaskState
    {
        TaskStateReport ToReport();
    }
}
