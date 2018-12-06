using System;
using System.Runtime.Serialization;
using Newtonsoft.Json;
using FWServiceAdmin.Code;

namespace FWServiceAdmin.Models
{

    /// <summary>
    /// Microservice's state object, decorated with Extended Attribute 
    /// </summary>
    [DataContract]
    public class TaskStateReport
    {
        [DataMember]
        [Extended("Emergency Stopped", false, 100)]
        public bool EmergencyStopped { get; set; }
        [DataMember]
        [Extended("Frozen", false, 100)]
        public bool Freezed { get; set; }
        [DataMember]
        [Extended("In Internal Process", false, 100)]
        public bool InInternalProcess { get; set; }
        [DataMember]
        [Extended("In Process", false, 100)]
        public bool InProcess { get; set; }
        [DataMember]
        [Extended("Task was cancelled", false, 100)]
        public bool ActionCancelled { get; set; }
        [DataMember]
        [Extended("Task started (Prev)", "1/1/0001 12:00:00 AM", typeof(DateTime), 2)]
        public DateTime LastStartDatePrevious { get; set; }
        [DataMember]
        [Extended("Task finished (Prev)", "1/1/0001 12:00:00 AM", typeof(DateTime), 2)]
        public DateTime LastEndDatePrevious { get; set; }
        [DataMember]
        [Extended("Task started", "1/1/0001 12:00:00 AM", typeof(DateTime), 2)]
        public DateTime LastStartDate { get; set; }
        [DataMember]
        [Extended("Task finished", "1/1/0001 12:00:00 AM", typeof(DateTime), 2)]
        public DateTime LastEndDate { get; set; }
        [DataMember]
        [Extended("Process finished with Error", false, 100)]
        public bool ExitedWithError { get; set; }
        [DataMember]
        [Extended("Request for new task started", "1/1/0001 12:00:00 AM", typeof(DateTime), 2)]
        public DateTime StartRequestExecuteDate { get; set; }
        [DataMember]
        [Extended("Request for new task finished", "1/1/0001 12:00:00 AM", typeof(DateTime), 2)]
        public DateTime EndRequestExecuteDate { get; set; }
        [DataMember]
        [Extended("Errors were raised", 0, 100)]
        public int ErrorsCount { get; set; }
        [DataMember]
        public string[] Exceptions { get; set; }

        /// <summary>
        /// status of microservice. will be used as [flag]
        /// </summary>
        /// <returns></returns>
        public int GetStatus()
        {
            return Freezed ? 2 : 0;
        }

        public string ToJson()
        {
            return JsonConvert.SerializeObject(this);
        }

        public static TaskStateReport FromJson(string data)
        {
            return JsonConvert.DeserializeObject<TaskStateReport>(data);
        }

    }
}
