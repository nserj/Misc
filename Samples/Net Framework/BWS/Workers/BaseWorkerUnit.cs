using NSHelpers.Data;
using System;
using System.ComponentModel;

namespace BaseWindowsService.Workers
{
    public class BaseWorkerUnit : ServiceBackgroundWorker, IDisposable
    {

        public event EventHandler OnError;
        public event EventHandler OnWorkerWarning;
        public event EventHandler OnDebug;

        public delegate void OnInprocessChangedHandler(object sender, EventArgs e);
        public event OnInprocessChangedHandler OnInprocessChanged;

        public BaseWorkerUnitArguments arguments { get; set; }
        public WorkerState State { get; set; }

        protected ServiceBackgroundWorker bgw;


         #region Raise Events


        protected virtual void RaiseOnError(Exception ex)
        {
            State.SetError(ex);
            OnError?.Invoke(this, EventArgs.Empty);
        }

        protected virtual void RaiseOnWorkerWarning(string msg)
        {
            State.WarningMessage = msg;
            OnWorkerWarning?.Invoke(this, EventArgs.Empty);
        }

        protected virtual void RaiseOnWorkerDebug(string msg)
        {
            if (arguments.DebugMode)
            {
                State.DebugMessage = msg;
                OnDebug?.Invoke(this, EventArgs.Empty);
            }
        }

        #endregion


        public BaseWorkerUnit()
        {
            CreateState();
        }

        protected void ClearVirtualStateFields(WorkerState ws)
        {
            ws.VirtualFields["Processing_ID"] = "";
            ws.VirtualFields["DataSetName"] = "";
            ws.VirtualFields["DataSet_ID"] = 0;
            ws.VirtualFields["AnalysisName"] = "";
            ws.VirtualFields["Analysis_ID"] = 0;
            ws.VirtualFields["User_ID"] = 0;
        }

        protected virtual void CreateState()
        {
            State = new WorkerState();
            ClearVirtualStateFields(State);
            State.OnStateChanged += State_OnStateChanged;
        }

        private void State_OnStateChanged(object sender, WorkerStateEventArgs e)
        {
           if (e.State == enmBWStateChanges.InProcess)
                OnInprocessChanged?.Invoke(this, EventArgs.Empty);
        }

        protected void ClearState()
        {
            State.Error = WSvcException.GetEmpty();
            State.WarningMessage = "";
            State.InfoMessage = "";
            State.Set_ActionCancelled(false);
            ClearVirtualStateFields(State);
        }


        public virtual void SetTask(BaseWorkerUnitArguments args)
        {

        }

        public virtual void RunTask(object sender, DoWorkEventArgs e)
        {
            bgw = (ServiceBackgroundWorker)sender;
            arguments = (BaseWorkerUnitArguments)e.Argument;

        }


        #region IDisposable Members

        public virtual void DoDispose(bool disposing)
        {

        }

        protected override void Dispose(bool disposing)
        {

            DoDispose(disposing);

            if (disposing)
            {
                if (!State.Disposed)
                {
                    if (arguments != null)
                    {
                        if (arguments.fq != null)
                            arguments.fq.Dispose();

                        arguments = null;
                    }
                }
                State.Disposed = true;
            }

            base.Dispose(disposing);
        }

        #endregion

    }


    public class BaseWorkerUnitArguments
    {
        public FQO fq { get; set; }
        public bool DebugMode { get; set; }

    }


}
