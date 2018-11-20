using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Serialization;

namespace BaseWindowsService.Workers
{

    public enum enmUpdateCommandState
    {
        Unset=0,
        Planned=1,
        InProgress = 2
    }

    public class WorkerStateEventArgs:EventArgs
    {
        public enmBWStateChanges State;
        public WorkerStateEventArgs() { }
        public WorkerStateEventArgs(enmBWStateChanges state) { State = state; }
    }


    [Serializable, XmlRoot(ElementName = "WorkerStates")]
    public class WorkerStates
    {

        public WorkerStates() { }

        [XmlArrayItem("WorkerState")]
        public List<WorkerState> Items = new List<WorkerState>();

        public WorkerState GetState(string name)
        {
            return Items.FirstOrDefault(u => string.Compare(u.Name, name, StringComparison.OrdinalIgnoreCase) == 0);
        }

        public void RestoreLastDates()
        {
            foreach (WorkerState ws in Items)
                ws.RestoreLastDates();
        }


    }


    [Serializable]
    public class WorkerState
    {

        public delegate void WorkerStateChangedEventHandler(object sender, WorkerStateEventArgs e);

        public event WorkerStateChangedEventHandler OnStateChanged;

        public WorkerState() { }

        public WorkerState(string name)
        {
            Name = name;
        }

        private bool _inInternalProcess;
        private WSvcException _error = WSvcException.GetEmpty();
        private DateTime _LastStartDatePreviousBuf;
        private DateTime _LastEndDatePreviousBuf;
        private bool _InStopState;
        private bool _InHoldState;
        private bool _ActionCancelled;
        private bool _InProcess;

        [XmlAttribute]
        public string Name { get; set; }
        [XmlAttribute]
        public DateTime LastStartDatePrevious { get; set;}
        [XmlAttribute]
        public DateTime LastStartDate { get; set; }
        [XmlAttribute]
        public DateTime LastEndDatePrevious { get; set; }
        [XmlAttribute]
        public DateTime LastEndDate { get; set; }
        [XmlAttribute]
        public bool ExitedWithError { get { return (Error != null && !Error.IsEmpty); } }
        /// <summary>
        /// date when request to check command was started
        /// </summary>
        [XmlAttribute]
        public DateTime StartRequestExecuteDate { get; set; }
        /// <summary>
        /// date when request to check command was finished
        /// </summary>
        [XmlAttribute]
        public DateTime EndRequestExecuteDate { get; set; }


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

        #region XML Ignore
        protected Object thisLock = new Object();

        [XmlIgnore]
        public Dictionary<string, object> VirtualFields { get; } = new Dictionary<string, object>();
        [XmlIgnore]
        public string CurrentProcessName { get; set; } = "";
        [XmlIgnore]
        public enmUpdateCommandState RunImmediately_Directive { get; set; } = enmUpdateCommandState.Unset;
        [XmlIgnore]
        public DateTime BaseConfigurationLoadedDate { get; set; }
        [XmlIgnore]
        public DateTime ExternalConfigurationLoadedDate { get; set; }
        [XmlIgnore]
        public bool IsErrorProcessed { get; set; }
        [XmlIgnore]
        public bool InBatchMode { get; set; }
        [XmlIgnore]
        public bool InInternalProcess
        {
            get { return _inInternalProcess; }
            set
            {
                bool old;
                lock (thisLock)
                {
                    old = _inInternalProcess;
                    _inInternalProcess = value;
                }

                if (old != _inInternalProcess)
                    OnStateChanged?.Invoke(this, new WorkerStateEventArgs(enmBWStateChanges.InInternalProcess));
            }
        }
        [XmlIgnore]
        public int Index { get; set; } = -1;

        [XmlIgnore]
        public bool InProcess
        {
            get { return _InProcess; }
            set
            {
                bool old = _InProcess;
                _InProcess = value;

                if (old != _InProcess)
                    OnStateChanged?.Invoke(this, new WorkerStateEventArgs(enmBWStateChanges.InProcess));
            }
        }

        [XmlIgnore]
        public bool InUnitsProcess { get; set; }

        [XmlIgnore]
        public bool InStopState
        {
            get { return _InStopState; }
            set
            {
                bool old = _InStopState;
                _InStopState = value;

                if (old != _InStopState)
                    OnStateChanged?.Invoke(this, new WorkerStateEventArgs(enmBWStateChanges.Stop));
            }
        }

        [XmlIgnore]
        public bool Disposed { get; set; } = false;
        [XmlIgnore]
        public string WarningMessage { get; set; }
        [XmlIgnore]
        public string InfoMessage { get; set; }
        [XmlIgnore]
        public string DebugMessage { get; set; }
        [XmlIgnore]
        public int User_ID { get; set; }

        [XmlIgnore]
        public bool InHoldState
        {
            get { return _InHoldState; }
            set
            {
                bool old = _InHoldState;
                _InHoldState = value;

                if (old != _InHoldState)
                    OnStateChanged?.Invoke(this, new WorkerStateEventArgs(enmBWStateChanges.Hold));
            }
        }

        [XmlIgnore]
        public WSvcException Error
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


        [XmlIgnore]
        public int LastError_QueueLength { get; set; } = 5;
        [XmlIgnore]
        public List<WSvcException> LastError { get; private set; } = new List<WSvcException>();
        [XmlIgnore]
        public bool ActionCancelled
        {
            get { return _ActionCancelled; }
            set
            {

                bool old = _ActionCancelled;
                _ActionCancelled = value;

                if (old != _ActionCancelled)
                    OnStateChanged?.Invoke(this, new WorkerStateEventArgs(enmBWStateChanges.CanceledCT));
            }
        }

        [XmlIgnore]
        public int AttemptsMade { get; set; } = 0;
        [XmlIgnore]
        public bool BlockedByAttempts { get; set; }
        [XmlIgnore]
        public bool IsDefaultTimerInterval { get; set; } = true;
        /// <summary>
        /// Thread currently run DoAction. DoAction will not be finished until HandleCallback will not reset this flag
        /// </summary>
        [XmlIgnore]
        public bool InSQLAction { get; set; } = false;


        #endregion

        public void Set_ActionCancelled(bool val)
        {
            _ActionCancelled = val;
        }

        protected void LastError_Collect()
        {
            if (!Error.IsEmpty)
                return;

            if (LastError.Count + 1 > LastError_QueueLength)
                LastError.RemoveRange(0, LastError.Count + 1 - LastError_QueueLength);

            LastError.Add(Error.Copy());

        }

        public virtual void SetError(int num)
        {
            LastError_Collect();

            Error.Set(num);
            IsErrorProcessed = false;
        }

        public virtual void SetError( Exception ex)
        {
            SetError(WSvcException.HANDLED_ERROR, ex);
        }

        public virtual void SetError(int num, Exception ex)
        {
            LastError_Collect();

            Error.Set(num, ex);
            IsErrorProcessed = false;
        }

        public virtual void SetError(string msg, Exception ex, params object[] prm)
        {
            if (prm != null && prm.Length > 0 && !string.IsNullOrWhiteSpace(msg))
                msg = string.Format(msg, prm);

            LastError_Collect();

            Error.Set(WSvcException.HANDLED_ERROR, msg, ex);
            IsErrorProcessed = false;
        }


        /// <summary>
        /// Set Hold state value, do not raise event
        /// </summary>
        /// <param name="hstate"></param>
        public void SetInHoldState(bool hstate)
        {
            _InHoldState = hstate;
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

        public virtual string GetStateMessage(enmLogSeverity severity)
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



    }
}
