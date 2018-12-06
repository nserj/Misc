using System;
using System.Runtime.Serialization;


namespace DP2SaaSMCS.Tasks
{

    /// <summary>
    /// Request/Response data class with Admin site.
    /// Describe current state of a taks
    /// It will be decorated with additional Attribute on Admimn site side.
    /// </summary>
    [DataContract]
    public class TaskStateReport
    {
        [DataMember]
        public bool EmergencyStopped { get; set; }
        [DataMember]
        public bool Freezed { get; set; }
        [DataMember]
        public bool InInternalProcess { get; set; }
        [DataMember]
        public bool InProcess { get; set; }
        [DataMember]
        public bool ActionCancelled { get; set; }
        [DataMember]
        public DateTime LastStartDatePrevious { get; set; }
        [DataMember]
        public DateTime LastEndDatePrevious { get; set; }
        [DataMember]
        public DateTime LastStartDate { get; set; }
        [DataMember]
        public DateTime LastEndDate { get; set; }
        [DataMember]
        public bool ExitedWithError { get; set;}
        [DataMember]
        public DateTime StartRequestExecuteDate { get; set; }
        [DataMember]
        public DateTime EndRequestExecuteDate { get; set; }
        [DataMember]
        public int ErrorsCount { get; set; }
        [DataMember]
        public string[] Exceptions { get; set; }

    }
}
