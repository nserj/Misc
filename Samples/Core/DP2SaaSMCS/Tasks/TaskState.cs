using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using ScheduledService.Code;
using System.ComponentModel;

namespace DP2SaaSMCS.Tasks
{

    /// <summary>
    /// Describe current state of a task
    /// </summary>
    [DataContract]
    public class TaskState : ScheduledTaskState, INotifyPropertyChanged
    {

        public TaskState() { }
        public TaskState(IMCSException error)
        {
            _error = error;
        }

        public event PropertyChangedEventHandler PropertyChanged;

        protected virtual void RaisePropertyChanged([System.Runtime.CompilerServices.CallerMemberName] string propertyName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }

        protected object thisLock = new object();

        private bool _EmergencyStopped;

        [DataMember]
        public virtual bool EmergencyStopped
        {
            get { return _EmergencyStopped; }
            set
            {
                if (value != _EmergencyStopped)
                {
                    _EmergencyStopped = value;
                    RaisePropertyChanged();
                }
            }
        }

        private bool _Freezed;

        [DataMember]
        public virtual bool Freezed
        {
            get { return _Freezed; }
            set
            {
                bool old = _Freezed;
                _Freezed = value;

                if (value != _Freezed)
                {
                    _Freezed = value;
                    RaisePropertyChanged();
                }
            }
        }

        private bool _inInternalProcess;

        [DataMember]
        public virtual bool InInternalProcess
        {
            get { return _inInternalProcess; }
            set
            {
                lock (thisLock)
                {
                    if (value != _inInternalProcess)
                    {
                        _inInternalProcess = value;
                        RaisePropertyChanged();
                    }
                }
            }
        }

        private bool _InProcess;

        [DataMember]
        public virtual bool InProcess
        {
            get { return _InProcess; }
            set
            {
                if (value != _InProcess)
                {
                    _InProcess = value;
                    RaisePropertyChanged();
                }
            }
        }

        private bool _ActionCancelled;

        [DataMember]
        public virtual bool ActionCancelled
        {
            get { return _ActionCancelled; }
            set
            {
                if (value != _ActionCancelled)
                {
                    _ActionCancelled = value;
                    RaisePropertyChanged();
                }
            }
        }

        public void Set_ActionCancelled(bool val)
        {
            _ActionCancelled = val;
        }

        /// <summary>
        /// Set Freezed state value, do not raise event
        /// </summary>
        /// <param name="hstate"></param>
        public void SetInFreezedState(bool hstate)
        {
            _Freezed = hstate;
        }

        /// <summary>
        /// Set Process state value, do not raise event
        /// </summary>
        /// <param name="pstate"></param>
        public void SetInProcess(bool pstate)
        {
            _InProcess = pstate;
        }

        /// <summary>
        /// Set InernalProcess state value, do not raise event
        /// </summary>
        /// <param name="pstate"></param>
        public void SetInInernalProcess(bool pstate)
        {
            _inInternalProcess = pstate;
        }




        private IMCSException _error;
        private DateTime _LastStartDatePreviousBuf;
        private DateTime _LastEndDatePreviousBuf;

        [DataMember]
        public string Name { get; set; }
        [DataMember]
        public DateTime LastStartDatePrevious { get; set; }
        [DataMember]
        public DateTime LastStartDate { get; set; }
        [DataMember]
        public DateTime LastEndDatePrevious { get; set; }
        [DataMember]
        public DateTime LastEndDate { get; set; }
        [DataMember]
        public bool ExitedWithError { get { return (Error != null && !Error.IsEmpty); } }


        /// <summary>
        /// Date when action started
        /// </summary>
        /// <param name="dt"></param>
        public void SetLastStartDate(DateTime dt)
        {
            _LastStartDatePreviousBuf = LastStartDatePrevious;
            LastStartDatePrevious = LastStartDate;
            LastStartDate = dt;
        }

        /// <summary>
        /// Date when action finished
        /// </summary>
        /// <param name="dt"></param>
        public void SetLastEndDate(DateTime dt)
        {
            _LastEndDatePreviousBuf = LastEndDatePrevious;
            LastEndDatePrevious = LastEndDate;
            LastEndDate = dt;
        }

        public void RestoreLastDates()
        {
            LastStartDate = LastStartDatePrevious;
            LastStartDatePrevious = _LastStartDatePreviousBuf;
            LastEndDate = LastEndDatePrevious;
            LastEndDatePrevious = _LastEndDatePreviousBuf;
        }


        public Dictionary<string, object> VirtualFields { get; } = new Dictionary<string, object>();

        public string CurrentProcessName { get; set; } = "";

        public bool IsErrorProcessed { get; set; }

        public bool InBatchMode { get; set; }

        public int Index { get; set; } = -1;


        public bool Disposed { get; set; } = false;
        public string WarningMessage { get; set; }
        public string InfoMessage { get; set; }
        public string DebugMessage { get; set; }
        public int User_ID { get; set; }


        public IMCSException Error
        {
            get
            {
                return _error;
            }
            set
            {
                _error = value;
                IsErrorProcessed = false;
            }
        }


        public int LastError_QueueLength { get; set; } = 5;
        public List<IMCSException> LastError { get; private set; } = new List<IMCSException>();


        public int AttemptsMade { get; set; } = 0;
        public bool BlockedByAttempts { get; set; }



        protected void LastError_Collect()
        {
            if (Error.IsEmpty)
                return;

            if (LastError.Count + 1 > LastError_QueueLength)
                LastError.RemoveRange(0, LastError.Count + 1 - LastError_QueueLength);

            LastError.Add(Error.Copy());

        }

        public void SetError(int num)
        {
            Error.Set(num);
            LastError_Collect();
            IsErrorProcessed = false;
        }

        public void SetError(Exception ex)
        {
            SetError(MCSException.HANDLED_ERROR, ex);
        }

        public void SetError(int num, Exception ex)
        {
            Error.Set(num, ex);
            LastError_Collect();
            IsErrorProcessed = false;
        }

        public void SetError(string msg, Exception ex, params object[] prm)
        {
            if (prm != null && prm.Length > 0 && !string.IsNullOrWhiteSpace(msg))
                msg = string.Format(msg, prm);

            Error.Set(MCSException.HANDLED_ERROR, msg, ex);
            LastError_Collect();
            IsErrorProcessed = false;
        }



        public string GetStateMessage(enmLogSeverity severity)
        {
            switch (severity)
            {
                case enmLogSeverity.Default:
                    return Error.Message;
                case enmLogSeverity.Info:
                    return (InfoMessage ?? "");
                case enmLogSeverity.Warning:
                    return (WarningMessage ?? "");
            }

            return "";
        }


        public void Clear()
        {
            Error.Clear();
            WarningMessage = "";
            InfoMessage = "";
            Set_ActionCancelled(false);
        }

        public TaskStateReport ToReport()
        {
            TaskStateReport rp = new TaskStateReport()
            {
                ActionCancelled = ActionCancelled,
                Freezed = Freezed,
                EmergencyStopped = EmergencyStopped,
                ExitedWithError = ExitedWithError,
                EndRequestExecuteDate = EndRequestExecuteDate,
                InInternalProcess = InInternalProcess,
                InProcess = InProcess,
                LastEndDate = LastEndDate,
                LastEndDatePrevious = LastEndDatePrevious,
                LastStartDate = LastStartDate,
                LastStartDatePrevious = LastStartDatePrevious,
                StartRequestExecuteDate = StartRequestExecuteDate,
                ErrorsCount = LastError.Count
            };

            string[] exceptions = new string[LastError.Count];

            for (int i = 0; i < LastError.Count; i++)
            {
                if (!string.IsNullOrWhiteSpace(LastError[i].Message))
                {
                    exceptions[i] = string.Format("[{0}]: {1}", LastError[i].TimeStamp.ToString("MM/dd/yyyy hh:mm:ss tt"), LastError[i].Message);
                }

            }

            rp.Exceptions = exceptions.Where(u => !string.IsNullOrWhiteSpace(u)).ToArray();

            return rp;

        }

    }
}
