using NSHelpers.Data;
using NSHelpers.Reflection;
using NSHelpers.XML;
using System;
using System.ComponentModel;
using System.Data;
using System.IO;
using System.Threading;
using System.Collections.Generic;

namespace BaseWindowsService.Workers
{

    public class BaseWorker : ServiceBackgroundWorker, IEquatable<BaseWorker>, IDisposable
    {
        protected const int EMPTY_RETURN_VALUE = -100000;

        protected System.Timers.Timer tmrActions;

        protected EventWaitHandle _blockerHndl = new EventWaitHandle(false, EventResetMode.AutoReset);
        protected EventWaitHandle _blockerHndlInProcess = new EventWaitHandle(false, EventResetMode.AutoReset);
        protected EventWaitHandle _blockerThreadBatchMode = new EventWaitHandle(true, EventResetMode.AutoReset);

        protected object lockTimerEnable = new object();
        protected Object thisUnitsLock = new Object();

        protected Type ExternalConfigType;
        protected int Attempts_State;

        /// <summary>
        /// Will this class manage internal state flag 
        /// </summary>
        protected bool AutomaticallyManageInternalProcess { get; set; } = true;

        public delegate void WorkerStruEventHandler(object sender, WorkerEventArgs e);
        public delegate void OnErrorEventHandler(object sender, OnErrorEventArgs e);
        public delegate void OnWorkerInfoEventHandler(object sender, OnWorkerInfoEventArgs e);
        public delegate void OnWorkerWarningEventHandler(object sender, OnWorkerInfoEventArgs e);
        public delegate void OnWorkerDebugEventHandler(object sender, OnWorkerInfoEventArgs e);
        public delegate void OnWorkerDetailsEventHandler(object sender, OnWorkerDetailsEventArgs e);
        public delegate void WorkerSendSMTP(object sender, string message, enmLogSeverity severity);
        public delegate void WorkerSendSMTPWithAttachment(object sender, string message, string data, string filename, enmLogSeverity severity);
        public delegate void OnWorkerStateEventHandler(object sender, OnWorkerStateEventArgs e);

        public event WorkerStruEventHandler OnGetWorkerStru;
        public event OnErrorEventHandler OnError;
        public event EventHandler OnStart;
        public event EventHandler OnComplete;
        public event OnWorkerInfoEventHandler OnWorkerInfo;
        public event OnWorkerWarningEventHandler OnWorkerWarning;
        public event OnWorkerDebugEventHandler OnDebug;
        public event OnWorkerDetailsEventHandler OnDetails;
        public event WorkerSendSMTP OnSendSMTP;
        public event WorkerSendSMTPWithAttachment OnSendSMTPWithAttachment;
        public event EventHandler OnSaveState;
        public event OnWorkerStateEventHandler OnWorkerState;


        public int ExecuteReturnValue;
        public DataTable ExecuteReturnTable;

        public WorkerConfig Cfg { get; set; }
        public WorkerConfig CfgPendingToChange { get; set; }
        public IStepsConfig ExternalCfgPendingToChange { get; set; }
        public IStepsConfig ExternalTaskCommands;

        public List<BaseWorkerUnit> Units { get; set; } = new List<BaseWorkerUnit>();



        #region Cancelation Token

        protected Object CancelationTokenLock = new Object();
        protected CancellationTokenSourceExt actionCancelationToken { get; private set; }

        protected void actionCancelationToken_Open()
        {
            lock (CancelationTokenLock)
            {
                actionCancelationToken = new CancellationTokenSourceExt();
                actionCancelationToken.OnCancel += _actionCancelationToken_OnCancel;
                State.ActionCancelled = false;
            }
        }

        protected void actionCancelationToken_Close()
        {
            lock (CancelationTokenLock)
            {
                if (actionCancelationToken != null)
                {
                    actionCancelationToken.Dispose();
                    actionCancelationToken = null;
                }
            }
        }


        private void _actionCancelationToken_OnCancel(object sender, EventArgs e)
        {
            State.ActionCancelled = true;
        }


        public void CancelationToken_SetCancel()
        {
            lock (CancelationTokenLock)
            {
                if (actionCancelationToken != null && !actionCancelationToken.IsCancellationRequested)
                {
                    actionCancelationToken.Cancel();
                }
            }
        }

        #endregion


        public BaseWorker() { }

        public void Init(WorkerConfig cfg)
        {
            UniqueName = cfg.Name;

            Cfg = cfg;
            SetupStates();
            SetTimer();
        }


        ~BaseWorker()
        {
            Dispose(false);
        }


        #region Units

        public virtual List<bool> GetCurrentUnitsState()
        {
            List<bool> lst = new List<bool>();

            lock (thisUnitsLock)
            {
                if (Units != null)
                {
                    for (int i = 0; i < Units.Count; i++)
                    {
                        if (Units[i] != null)
                            lst.Add((Units[i].State.InProcess || !Units[i].WorkCompleted));
                    }
                }
            }

            return lst;
        }

        public virtual int UnitsInProcessCount()
        {
            int cnt = 0;

            lock (thisUnitsLock)
            {
                if (Units != null)
                {
                    for (int i = 0; i < Units.Count; i++)
                    {
                        if (Units[i] != null && Units[i].State.InProcess)
                            cnt++;
                    }
                }
            }

            return cnt;
        }


        #endregion

        #region States

        protected WorkerStates _States;

        public WorkerStates States
        {
            get { return _States; }
        }

        protected void State_OnStateChanged(object sender, WorkerStateEventArgs e)
        {
            StateChanged(e.State);
            OnWorkerState?.Invoke(this, new OnWorkerStateEventArgs(e.State));
        }


        public virtual void ClearState()
        {
            ClearState(State);
        }

        public virtual void ClearState(WorkerState st)
        {
            st.Error = WSvcException.GetEmpty();
            st.WarningMessage = "";
            st.InfoMessage = "";
            st.Set_ActionCancelled(false);
        }

        public virtual WorkerState AddNewState(string name)
        {
            WorkerState ws = States.GetState(name);
            if (ws == null)
            {
                ws = new WorkerState(name);
                States.Items.Add(ws);
            }

            return ws;
        }

        public virtual void SetupStates()
        {
            _States = new WorkerStates();
            AddNewState(Cfg.Name);
            State.OnStateChanged += State_OnStateChanged;
        }

        public virtual void SetupStates(WorkerStates sts)
        {
            _States = sts;
            State.OnStateChanged += State_OnStateChanged;
        }

        public override void StateChanged(enmBWStateChanges statetype)
        {
            if (statetype == enmBWStateChanges.CancelledBW ||
                (statetype == enmBWStateChanges.Hold && State.InHoldState) ||
                statetype == enmBWStateChanges.Stop)
            {
                CancelationToken_SetCancel();
            }

            base.StateChanged(statetype);

            if (statetype == enmBWStateChanges.Hold)
            {
                if (!State.InHoldState && !ImmediatelyStop)
                {
                    if (State.BlockedByAttempts)
                        State.BlockedByAttempts = false;
                    StartTimer();
                }
                else
                {
                    StopTimer();
                }
            }
        }

        public WorkerState State
        {
            get { return States.Items[0]; }
        }

        /// <summary>
        /// Set Worker in hold state
        /// </summary>
        /// <param name="value">bool, state</param>
        /// <param name="suppressEvent">raise or not an event</param>
        public void SetInHoldState(bool value, bool suppressEvent = false)
        {
            if (!suppressEvent)
                State.InHoldState = value;
            else
                State.SetInHoldState(value);
        }

        public void SetStopState()
        {
            State.InStopState = true;
        }

        public bool BreakIfAny
        {
            get { return (BreakIfSystem || State.InProcess); }
        }

        public bool BreakIfSystem
        {
            get
            {
                try
                {
                    return (State.InHoldState || ImmediatelyStop || State.BlockedByAttempts);
                }
                catch
                {
                    return true;
                }
            }
        }

        public bool ImmediatelyStop
        {
            get { return (State.Disposed || CancellationPending || State.InStopState); }
        }

     /*   protected virtual void ForcedlyHold(string src, string msg)
        {
            SetInHoldState(true);

            if (string.IsNullOrWhiteSpace(msg))
                msg = "The thread was put in hold state since previous operation was executed with heavy error. The thread will not be functional until a problem will be resolved.";

            msg = string.Format("SRC: {0}, {1}", src, msg);

            RaiseOnWorkerWarning(msg);

        }*/

        public virtual bool SetRunImmediatelly()
        {
            State.RunImmediately_Directive = enmUpdateCommandState.Unset;

            if (State.InHoldState || ImmediatelyStop || State.InInternalProcess || State.InUnitsProcess)
                return false;

            State.RunImmediately_Directive = enmUpdateCommandState.Planned;

            return true;
        }

        public virtual void ReRunImmediatelly()
        {
            lock (lockTimerEnable)
            {
                if (Attempts_State == 1)
                    return;

                StopTimer();
                State.RunImmediately_Directive = enmUpdateCommandState.InProgress;
                ResetAll(true);
                State.BlockedByAttempts = false;
                SetTimerInterval(1000, true);
            }
        }

        #endregion

        #region Timer

        protected void StopTimer()
        {
            if (State.Disposed)
                return;

            tmrActions.Enabled = false;
        }

        public void StartTimer()
        {
            if (State.Disposed)
                return;

            tmrActions.Enabled = false;
            if (!BreakIfSystem)
                tmrActions.Enabled = true;
        }

        protected void SetTimerInterval(int interval, bool autostart)
        {
            if (State.Disposed)
                return;

            if (!BreakIfSystem)
            {
                if (autostart && tmrActions.Enabled)
                    tmrActions.Enabled = false;

                tmrActions.Interval = interval;
                State.IsDefaultTimerInterval = false;

                if (autostart)
                    tmrActions.Enabled = true;
            }
        }

        protected void SetTimer()
        {
            switch (Cfg.ExecuteCondition.ExecIntervalType)
            {
                case wcExecIntervalType.Second:
                    Cfg.TimeMultiplier = Cfg.ExecuteCondition.ExecInterval * 1000;
                    break;
                case wcExecIntervalType.Minute:
                    Cfg.TimeMultiplier = Cfg.ExecuteCondition.ExecInterval * 60 * 1000;
                    break;
                case wcExecIntervalType.Millisecond:
                    Cfg.TimeMultiplier = Cfg.ExecuteCondition.ExecInterval;
                    break;
                case wcExecIntervalType.Weekly:
                    Cfg.TimeMultiplier = 1000;
                    Cfg.ExecuteCondition.ExecInterval = 1000;
                    break;
            }

            if (Cfg.TimeMultiplier == 0) Cfg.TimeMultiplier = 30000;

            if (tmrActions == null)
            {
                tmrActions = new System.Timers.Timer(Cfg.TimeMultiplier);
                tmrActions.Elapsed += new System.Timers.ElapsedEventHandler(OnTimeElapsed);
            }
            else
                tmrActions.Interval = Cfg.TimeMultiplier;

            State.IsDefaultTimerInterval = true;
        }

        protected virtual void OnTimeElapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            StopTimer();

            if (!BreakIfAny)
            {
                try
                {
                    ClearState(State);
                    State.InProcess = true;

                    DoActions();

                }
                catch (Exception ex)
                {
                    RaiseOnError(ex);
                }
                finally
                {

                    Attempts_State = CheckAttempts();

                    switch(Attempts_State)
                    {
                        case 1:  // try to run process for next attempt
                            if (State.RunImmediately_Directive != enmUpdateCommandState.InProgress)
                                State.RunImmediately_Directive = enmUpdateCommandState.InProgress;

                            ResetAll();
                            SetTimerInterval(200, true);
                            break;
                        case -1:
                        case 2:
                            State.InProcess = false;
                            break;
                        case 0:
                            lock(lockTimerEnable)
                            {
                                if (Cfg.ExecuteCondition.Runimmediately)
                                    Cfg.ExecuteCondition.Runimmediately = false;

                                if (State.RunImmediately_Directive == enmUpdateCommandState.InProgress)
                                    State.RunImmediately_Directive = enmUpdateCommandState.Unset;

                                if (!State.IsDefaultTimerInterval)
                                    SetTimer();

                                ResetAll();
                                StartTimer();
                            }
                            break;
                    }

                }
            }
        }

        #endregion

        #region Config

        public virtual void ChangeConfig( bool suppressEvents, bool startafterchanges)
        {

            if (CfgPendingToChange == null && ExternalCfgPendingToChange==null)
                throw new Exception("Config which pending to change is null");

            bool update_base_cfg = ((CfgPendingToChange.ReloadConfigType & wcConfigLoadType.Base) > 0);
            bool update_external_cfg = ((CfgPendingToChange.ReloadConfigType & wcConfigLoadType.External) > 0);

            bool inhold = update_base_cfg ? CfgPendingToChange.ExecuteCondition.StartFreezed : State.InHoldState;

            StopTimer();
            SetInHoldState(true, true);
            Thread.Sleep(100);

            bool suppress_smtp_all = update_base_cfg ? CfgPendingToChange.SuppressAllSMTP : Cfg.SuppressAllSMTP;
            bool suppress_smtp_info = update_base_cfg ? CfgPendingToChange.SuppressInformationSMTP : Cfg.SuppressInformationSMTP;

            int load_external_result = -100;

            try
            {

                if (update_base_cfg)
                {
                    Cfg = CfgPendingToChange;
                    State.BaseConfigurationLoadedDate = DateTime.Now;
                }

                Cfg.SuppressInformationSMTP = false;
                Cfg.SuppressAllSMTP = false;

                if (update_external_cfg)
                    load_external_result = LoadExternalConfig();

                SetTimer();
                ResetAll(suppressEvents);

//                if (update_base_cfg)
                    State.BlockedByAttempts = false;

                if (!inhold)
                    SetInHoldState(false, true);

                if (update_base_cfg || load_external_result == 1)
                {
                   string cfg_body = XmlManager.FormatXmlText(XmlManager.SerializeToString(Cfg, false));

                    if (load_external_result >= 0)
                    {
                        cfg_body = cfg_body + "\n\n";
                        cfg_body = cfg_body + XmlManager.FormatXmlText(XmlManager.SerializeToString(ExternalTaskCommands, false));
                    }

                    RaiseOnSendSMTP(Utils.FormatForEmail(Cfg.Name, "Configuration was changed by request.", enmLogSeverity.Warning),
                        cfg_body, "Configs_"+Cfg.Name+".xml", enmLogSeverity.Warning);

                    cfg_body = XmlManager.RemoveXmlFormat(cfg_body);

                    RaiseOnWorkerWarning(string.Format("Configuration was changed by request: {0}", cfg_body), false, State);
                }
            }
            finally
            {
                Cfg.SuppressInformationSMTP = suppress_smtp_info;
                Cfg.SuppressAllSMTP = suppress_smtp_all;
                CfgPendingToChange = null;
                ExternalCfgPendingToChange = null;
            }

            if (startafterchanges)
                StartTimer();

        }

        public string ExternalConfigFilePath
        {
            get
            {
                return Path.Combine(Config.ConfigFilePath, Cfg.AdditionalConfigurationFile);
            }
        }

        public bool HaveExternalConfig
        {
            get
            {
                return (!string.IsNullOrWhiteSpace(Cfg.AdditionalConfigurationFileType) && !string.IsNullOrWhiteSpace(Cfg.AdditionalConfigurationFile));
            }
        }

        public IStepsConfig LoadExternalConfigFile()
        {
            string md5;
            IStepsConfig tmp_config = (IStepsConfig) XmlManager.DeserializeFromFile(ExternalConfigFilePath, ExternalConfigType, out md5);
            tmp_config.MD5 = md5;
            return tmp_config;
        }

        protected virtual int LoadExternalConfig()
        {
            if (!HaveExternalConfig)
                return -1;

            ExternalConfigType = Types.GetType(Utils.EntryAssemblyEName, Cfg.AdditionalConfigurationFileType);

            if (ExternalCfgPendingToChange == null)
                ExternalTaskCommands = LoadExternalConfigFile();
            else
                ExternalTaskCommands = ExternalCfgPendingToChange;

            State.ExternalConfigurationLoadedDate = DateTime.Now;
            return 1;
        }


        protected int CheckAttempts()
        {
            if (State.ExitedWithError)
            {
                if (State.AttemptsMade < Cfg.ExecuteCondition.AttemptsNumber)
                {
                    int waitforiter = 0;
                    while (waitforiter < Cfg.ExecuteCondition.DelayBetweenAttempts)
                    {
                        if (BreakIfSystem)
                        {
                            State.AttemptsMade = 0;
                            return -1;
                        }

                        Thread.Sleep(100);
                        waitforiter += 100;
                    }
                    State.AttemptsMade++;
                    return 1;
                }
                else
                {
                    State.BlockedByAttempts = true;

                    if(!State.IsErrorProcessed)
                    {
                        RaiseOnError(State);
                    }

                    RaiseOnWorkerWarning("Process was blocked since limit of attempts was exhausted.");
                    return 2;
                }
            }
            else if (State.AttemptsMade > 0)
                State.AttemptsMade = 0;

            return 0;
        }

        #endregion

 
        #region Raise Events

        protected bool CanWriteDebug
        {
            get
            {
                return ((Cfg.LogSeverity & enmLogSeverity.Debug) > 0 && OnDebug != null);
            }
        }

        protected virtual BaseWorker RaiseOnGetWorker(Type wtype)
        {
            WorkerEventArgs s = new WorkerEventArgs(wtype, null);

            OnGetWorkerStru?.Invoke(this, s);

            return s.obj;
        }

        protected virtual BaseWorker RaiseOnGetWorker(string uname)
        {
            WorkerEventArgs s = new WorkerEventArgs(uname, null);

            OnGetWorkerStru?.Invoke(this, s);

            return s.obj;
        }

        protected virtual void RaiseOnWorkerDebug(WorkerState currstate)
        {
            if (CanWriteDebug)
            {
                OnDebug?.Invoke(this, new OnWorkerInfoEventArgs(false, currstate));
            }
        }

        protected virtual void RaiseOnWorkerDebug(string msg) => RaiseOnWorkerDebug(msg, State);

        protected virtual void RaiseOnWorkerDebug(string msg, WorkerState currstate)
        {
            if (Cfg.WorkingMode == wcWorkingMode.Debug)
            {
                currstate.DebugMessage = msg;
                RaiseOnWorkerDebug(currstate);
            }
        }


        protected virtual void RaiseOnError(WorkerState currstate)
        {
            OnError?.Invoke(this, new OnErrorEventArgs(true, currstate));
            currstate.IsErrorProcessed = true;
        }


        protected virtual void RaiseOnError(Exception ex) => RaiseOnError(ex, true, State);

        protected virtual void RaiseOnError(Exception ex,  bool sendmail, WorkerState currstate) => RaiseOnError(ex, sendmail, "", currstate, null);

        protected virtual void RaiseOnError(string msg, params object[] prm) => RaiseOnError(null, false, true, msg, State, prm);

        protected virtual void RaiseOnError(string msg, Exception ex, params object[] prm) => RaiseOnError(ex, true, msg, State, prm);

        protected virtual void RaiseOnError(Exception ex, bool sendmail, string msg, WorkerState currstate, params object[] prm)
        {
            currstate.SetError(msg, ex, prm);
            OnError?.Invoke(this, new OnErrorEventArgs(Cfg.SuppressAllSMTP ? false : sendmail, currstate));
            currstate.IsErrorProcessed = true;
        }

        protected virtual void RaiseOnWorkerWarning(WorkerState currstate)
        {
            OnWorkerWarning?.Invoke(this, new OnWorkerInfoEventArgs(true, currstate));
        }

        protected virtual void RaiseOnWorkerWarning(string msg) => RaiseOnWorkerWarning(msg, true, State);

        protected virtual void RaiseOnWorkerWarning(string msg, bool sendmail, WorkerState currstate)
        {
            currstate.WarningMessage = msg;
            OnWorkerWarning?.Invoke(this, new OnWorkerInfoEventArgs(Cfg.SuppressAllSMTP ? false : sendmail, currstate));
        }

        protected virtual void RaiseOnWorkerInfo(string msg, bool sendmail) => RaiseOnWorkerInfo(msg, sendmail, State);

        protected virtual void RaiseOnWorkerInfo(string msg, bool sendmail, WorkerState currstate)
        {
            currstate.InfoMessage = msg;
            OnWorkerInfo?.Invoke(this, new OnWorkerInfoEventArgs((Cfg.SuppressInformationSMTP || Cfg.SuppressAllSMTP) ? false : sendmail, currstate));
        }

        protected virtual void RaiseOnSendSMTP(string message, string attachment_data, string attachment_file, enmLogSeverity severity)
        {
            if (!Cfg.SuppressAllSMTP)
                OnSendSMTPWithAttachment?.Invoke(this, message, attachment_data, attachment_file, severity);
        }

        protected virtual void RaiseOnSendSMTP(string message, enmLogSeverity severity)
        {
            if (!Cfg.SuppressAllSMTP)
                OnSendSMTP?.Invoke(this, message, severity);
        }

        protected virtual void RaiseOnSaveState()
        {
            OnSaveState?.Invoke(this, EventArgs.Empty);
        }

        protected virtual void RaiseOnStart()
        {

            Cfg.PreviousIterationStartTime = Cfg.CurrentIterationStartTime;
            Cfg.CurrentIterationStartTime = DateTime.Now;

            if (Cfg.PreviousIterationStartTime == DateTime.MinValue)
                Cfg.PreviousIterationStartTime = Cfg.CurrentIterationStartTime;

            OnStart?.Invoke(this, EventArgs.Empty);
        }

        protected virtual void RaiseOnDetails(string command, DateTime dstart, DateTime dend)
        {
            if (OnDetails != null && (Cfg.LogSeverity & enmLogSeverity.Details) > 0)
                OnDetails(this, new OnWorkerDetailsEventArgs(command, dstart, dend));
        }

        protected void RaiseOnComplete()
        {
            OnCompleteRaised();

            Cfg.PreviousIterationEndTime = Cfg.CurrentIterationEndTime;
            Cfg.CurrentIterationEndTime = DateTime.Now;

            if (Cfg.PreviousIterationEndTime == DateTime.MinValue)
                Cfg.PreviousIterationEndTime = Cfg.CurrentIterationEndTime;

            OnComplete?.Invoke(this, EventArgs.Empty);
        }

        protected virtual void OnCompleteRaised()
        {

        }

        #endregion

        #region Virtual functions

        protected virtual FQO GetFQO()
        {
            FQO fq = new FQO(Cfg.SQLCommandCondition.ConnectionString)
            {
                AutoCloseConnection = false,
                CommandTimeout = Cfg.SQLCommandCondition.CommandTimeOut
            };
            return fq;
        }

        public virtual void DoWorkStarted()
        {
            LoadExternalConfig();
        }

        public void RunTask(object sender, DoWorkEventArgs e)
        {
            StartedDate = DateTime.Now;

            ResetAll();

            DoWorkStarted();

            if (Cfg.ExecuteCondition.Runimmediately)
                SetTimerInterval(1000, false);

            StartTimer();

            while (!_blockerHndl.WaitOne(100, true))
            {

                if (ImmediatelyStop)
                {
                    while (!State.Disposed && !_blockerHndlInProcess.WaitOne(100, true))
                    {
                        if (!State.InProcess)
                            break;
                    }
                    break;
                }
            }

            StopTimer();
            ResetAll();

            if (CancellationPending)
            {
                e.Cancel = true;
            }
        }

        protected virtual void DoActions() { }


        public virtual void ResetAll(bool suppressEvents = false)
        {
            actionCancelationToken_Close();
        }

        #endregion

        #region IEquatable<BaseWorker> Members

        public override bool Equals(object obj)
        {
            if (obj == null) return false;
            if (!obj.GetType().Equals(typeof(BaseWorker))) return false;
            return (string.CompareOrdinal(UniqueName, ((BaseWorker)obj).UniqueName) == 0);
        }

        bool IEquatable<BaseWorker>.Equals(BaseWorker other)
        {
            if (other == null) return false;
            return (string.CompareOrdinal(UniqueName, other.UniqueName) == 0);
        }

        public override int GetHashCode()
        {
            return UniqueName.GetHashCode();
        }


        #endregion


        #region IDisposable Members

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (!State.Disposed)
                {
                    DoDispose();
                    if (tmrActions != null) tmrActions.Dispose();
                    if (_blockerHndl != null) _blockerHndl.Dispose();
                    if (_blockerThreadBatchMode != null) _blockerThreadBatchMode.Dispose();
                    if (_blockerHndlInProcess != null) _blockerHndlInProcess.Dispose();

                    actionCancelationToken_Close();

                }
                State.Disposed = true;
            }

            base.Dispose(disposing);
        }

        protected virtual void DoDispose()
        {
        }

        #endregion
    }
}
