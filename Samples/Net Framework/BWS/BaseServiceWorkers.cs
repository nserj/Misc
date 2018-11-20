using BaseWindowsService.Workers;
using System;
using System.ComponentModel;
using System.Diagnostics;
using System.ServiceProcess;
using WindowsSvcMessage;

namespace BaseWindowsService
{

    public partial class BaseService : ServiceBase
    {

        private bool CanWriteToWorkerLog(enmLogSeverity reqSev, enmLogSeverity sevLevel)
        {
            switch (reqSev)
            {
                case enmLogSeverity.Default:
                case enmLogSeverity.Warning:
                    return true;
                case enmLogSeverity.Info:
                    return ((sevLevel & enmLogSeverity.Info) > 0);
                case enmLogSeverity.Debug:
                    return ((sevLevel & enmLogSeverity.Debug) > 0);
                case enmLogSeverity.Details:
                    return ((sevLevel & enmLogSeverity.Details) > 0);
                default:
                    return false;
            }
        }


        protected virtual void WriteWorkerLog(string sendername, string message, string _event, enmLogSeverity reqSev, enmLogSeverity allowSev)
        {

            if (CanWriteToWorkerLog(reqSev, allowSev))
            {
                switch (reqSev)
                {
                    case enmLogSeverity.Default:
                        errLogger.Error(sendername, string.Format("{0}", _event), new Exception(message));
                        break;
                    case enmLogSeverity.Info:
                    case enmLogSeverity.Details:
                    case enmLogSeverity.Warning:
                        errLogger.Info(sendername, string.Format("{0}: {1} - {2}", reqSev.ToString().ToUpper(), _event, message));
                        break;
                    case enmLogSeverity.Debug:
                        errLogger.Debug(sendername, string.Format("{0} - {1}", _event, message));
                        break;
                }
            }

        }

        protected virtual string GetPostfix(enmLogSeverity severity)
        {
            switch (severity)
            {
                case enmLogSeverity.Default:
                    return "[Error]";
                case enmLogSeverity.Info:
                    return "[Info]";
                case enmLogSeverity.Warning:
                    return "[Warning]";
            }
            return "";
        }



        void Process_worker_transmittable(WorkerConfig wcfg, enmLogSeverity severity, bool sendEmail, WorkerState currstate, [System.Runtime.CompilerServices.CallerMemberName] string memberName = "")
        {
            try
            {
                string postfix = GetPostfix(severity);

                WriteWorkerLog(wcfg.Name, Utils.FormatForLog(currstate,severity), memberName, severity, wcfg.LogSeverity);


                if (sendEmail)
                    SendMail(NotificationSystemSubjectName+" " + postfix, Utils.FormatForEmail(currstate,wcfg.FriendlyName, severity), severity,"","");

            }
            catch (Exception ex)
            {
                WriteSysLog(this.ServiceName + "." + memberName, string.Concat(Helpers.ExceptionToString(ex), " : ", Config.ConfigFilePath),
                            EventLogEntryType.Error,false);
            }

        }

        private void worker_OnWorkerState(object sender, OnWorkerStateEventArgs e)
        {

            if (e.State != enmBWStateChanges.InProcess)
                return;

            BaseWorker bw = (BaseWorker)sender;

            if(!bw.State.InHoldState && !bw.ImmediatelyStop && !bw.State.InProcess)
            {
                if (bw.CfgPendingToChange != null || bw.ExternalCfgPendingToChange!=null)
                {
                    lock (objWorkerConfig_Lock)
                    {
                        bw.ChangeConfig(true, !(bw.State.RunImmediately_Directive == enmUpdateCommandState.Planned));
                    }
                }

                if(bw.State.RunImmediately_Directive == enmUpdateCommandState.Planned)
                    bw.ReRunImmediatelly();
            }
        }

        private void worker_OnSaveState(object sender, EventArgs e)
        {
            SaveWorkerState((BaseWorker)sender);
        }

        private void worker_OnSendSMTPWithAttachment(object sender, string message, string data, string filename, enmLogSeverity severity)
        {
            SendMail(NotificationSystemSubjectName + " " + GetPostfix(severity), message, severity,data,filename);
        }

        private void worker_OnSendSMTP(object sender, string message, enmLogSeverity severity)
        {
            SendMail(NotificationSystemSubjectName + " " + GetPostfix(severity), message, severity,"","");
        }

        void worker_OnError(object sender, OnErrorEventArgs e)
        {
            Process_worker_transmittable(((BaseWorker)sender).Cfg, enmLogSeverity.Default, e.sendEMail,e.state);
        }

        void worker_OnWorkerInfo(object sender, OnWorkerInfoEventArgs e)
        {
            Process_worker_transmittable(((BaseWorker)sender).Cfg, enmLogSeverity.Info, e.send_mail, e.state);
        }

        void worker_OnWorkerWarning(object sender, OnWorkerInfoEventArgs e)
        {
            Process_worker_transmittable(((BaseWorker)sender).Cfg, enmLogSeverity.Warning, e.send_mail, e.state);
        }

        void worker_OnDebug(object sender, OnWorkerInfoEventArgs e)
        {
            BaseWorker bw = (BaseWorker)sender;
            WriteWorkerLog(bw.Cfg.Name, e.state.DebugMessage, "OnDebug", enmLogSeverity.Debug, bw.Cfg.LogSeverity);
        }

        void worker_OnDetails(object sender, OnWorkerDetailsEventArgs e)
        {
            BaseWorker bw = (BaseWorker)sender;
            WriteWorkerLog(bw.Cfg.Name, e.Command, "OnDetails", enmLogSeverity.Details, bw.Cfg.LogSeverity);
        }

        void worker_OnStart(object sender, EventArgs e)
        {
            BaseWorker bw = (BaseWorker)sender;
            WriteWorkerLog(bw.Cfg.Name, "Started", "OnStart", enmLogSeverity.Debug, bw.Cfg.LogSeverity);

            if (Environment.UserInteractive)
                Console.WriteLine("Started");

        }

        void worker_OnComplete(object sender, EventArgs e)
        {
            BaseWorker bw = (BaseWorker)sender;
            WriteWorkerLog(bw.Cfg.Name, "Finished", "OnComplete", enmLogSeverity.Debug, bw.Cfg.LogSeverity);

            if (Environment.UserInteractive)
                Console.WriteLine("Completed");
        }

        private void worker_WorkerCompleteRun(object sender, RunWorkerCompletedEventArgs e)
        {
            BaseWorker sw = (BaseWorker)sender;
            enmServiceControllerStatus st = GetStatus();

            if (sw != null)
            {
                if (e.Error != null)
                    WriteSysLog(sw.Cfg.Name + ".RunWorkerCompleted", Helpers.ExceptionToString(e.Error), EventLogEntryType.Error, false);
                else if (e.Cancelled && st == enmServiceControllerStatus.Running)
                {
                    WriteSysLog(sw.Cfg.Name + ".RunWorkerCompleted", "Canceled", EventLogEntryType.Warning, false);
                }
            }
        }

        void worker_OnGetWorkerStru(object sender, WorkerEventArgs e)
        {
            if (e.wtype == null)
                e.obj = GetWorkerStructure(e.worker_name);
            else
                e.obj = GetWorkerStructure(e.wtype);

        }

    }

}
