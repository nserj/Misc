using System;
using System.ComponentModel;


namespace BaseWindowsService.Workers
{

    public enum enmBWStateChanges
    {
        Hold = 1,
        Stop = 2,
        CancelledBW = 3,
        CanceledCT = 4,
        InProcess = 5,
        InInternalProcess = 6
    }

    public class ServiceBackgroundWorker : BackgroundWorker
    {
        public string UniqueName;
        public bool WorkCompleted;
        public int Index;

        public DateTime StartedDate { get; set; }

        public delegate void RunWorkerCompletedEventHandler(object sender, RunWorkerCompletedEventArgs e);

        public event RunWorkerCompletedEventHandler WorkerCompleteRun;


        public ServiceBackgroundWorker()
        {
            UniqueName = Guid.NewGuid().ToString("D");
            this.RunWorkerCompleted += ServiceBackgroundWorker_RunWorkerCompleted;
        }

        private void ServiceBackgroundWorker_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            WorkCompleted = true;
            WorkerCompleteRun?.Invoke(this, e);
        }

        public new void CancelAsync()
        {
            StateChanged(enmBWStateChanges.CancelledBW);
            base.CancelAsync();
        }

        public virtual void StateChanged(enmBWStateChanges statetype)
        {

        }


    }

    #region Event Classes


    public class OnWorkerStateEventArgs : EventArgs
    {
        public enmBWStateChanges State;
        public OnWorkerStateEventArgs() { }
        public OnWorkerStateEventArgs(enmBWStateChanges state) { State = state; }
    }

    public class OnErrorEventArgs : EventArgs
    {
        public bool sendEMail;
        public WorkerState state;

        public OnErrorEventArgs(bool _sendemail,WorkerState _state)
        {
            sendEMail = _sendemail;
            state = _state;
        }
    }

    public class OnWorkerDetailsEventArgs : EventArgs
    {
        public string Command;
        public DateTime StartDate;
        public DateTime EndDate;

        public OnWorkerDetailsEventArgs(string command, DateTime dstart, DateTime dend)
        {
            Command = command;
            StartDate = dstart;
            EndDate = dend;
        }
    }

    public class OnWorkerInfoEventArgs : EventArgs
    {
        public bool send_mail;
        public WorkerState state;

        public OnWorkerInfoEventArgs(bool sendmail, WorkerState _state)
        {
            send_mail = sendmail;
            state = _state;
        }
    }

    public class WorkerEventArgs : EventArgs
    {
        public BaseWorker obj;
        public string worker_name = "";
        public bool forceFreeze;
        public string reason = "By direction";
        public Type wtype = null;

        public WorkerEventArgs(string uname, string _reason)
        {
            worker_name = uname;
            if (!string.IsNullOrEmpty(_reason))
                reason = _reason;
        }

        public WorkerEventArgs(Type wt, string _reason)
        {
            wtype = wt;
            if (!string.IsNullOrEmpty(_reason))
                reason = _reason;
        }
    }


    #endregion

}
