using BaseWindowsService.WCFCommunication;
using BaseWindowsService.Workers;
using NSHelpers.Reflection;
using NSHelpers.XML;
using NSLogger;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.ServiceProcess;
using System.Threading;
using WindowsSvcCommunication;
using WndowsSvcRequest;
using WindowsSvcMessage;
using NSHelpers;


namespace BaseWindowsService
{

    [Flags]
    public enum enmLogSeverity
    {
        None = 0,
        Default = 2,
        Info = 4,
        Debug = 8,
        Details = 16,
        Warning = 32
    }



    public partial class BaseService : ServiceBase, IServiceRequestHandler
    {
        protected enmServiceControllerStatus ServiceState = enmServiceControllerStatus.Stopped;
        protected EventLog mainevtLog;
        protected enmLogSeverity LogSeverity;

        protected bool IsWorkersStopped
        {
            get { return workers == null || workers.Count == 0; }
        }

        protected List<BaseWorker> workers = new List<BaseWorker>();
        protected WorkerConfigList<WorkerConfig> workersConfigList;
        protected Logger errLogger;

        protected int WaitForThreadStop;

        protected SMTPConfig smtpmailCfg;

        protected string NotificationSystemSubjectName;

        /*  private FileWatcher cfgChangeWatcher;
          private bool useFileWatcher;*/
        protected object objWorkerConfig_Lock = new object();

        protected IDisposable wcfHost;
        protected string wcfHostURL;

        public BaseService()
        {
            InitializeComponent();

            AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;

            this.ServiceName = Config.GetAppSettingsValue<string>("ServiceName", "");
            mainevtLog = new EventLog(Config.GetAppSettingsValue<string>("EventLogInstallLog", ""))
            {
                Source = Config.GetAppSettingsValue<string>("EventLogInstallSource", "")
            };

            LoadInitConfig();
        }

        protected void LoadInitConfig()
        {
            WaitForThreadStop = Config.GetAppSettingsValue<int>("WaitForThreadStopSeconds", 5);

            if (WaitForThreadStop > 60 || WaitForThreadStop <= 0) /*maximum 1 minute*/
                WaitForThreadStop = 5;

            WaitForThreadStop *= 1000;

            errLogger = new Logger();

            AppSection<SMTPConfig> asec = new AppSection<SMTPConfig>("smtpsettings");
            smtpmailCfg = asec.Get();
        }

        private void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            WriteSysLog(this.ServiceName + ".UnhandledException", Helpers.ExceptionToString((Exception)e.ExceptionObject), EventLogEntryType.Error, false);
            errLogger.Error(this.ServiceName, string.Format("{0}", "UnhandledException"), (Exception)e.ExceptionObject);

            ExitCode = ((Exception)e.ExceptionObject).GetWin32Code();
            Stop();
        }


        #region functions

        public enmServiceControllerStatus GetStatus()
        {
            ServiceState = enmServiceControllerStatus.NotFound;

            ServiceController ctl = ServiceController.GetServices().FirstOrDefault(s => s.ServiceName == this.ServiceName);

            if (ctl != null)
            {
                ServiceState = (enmServiceControllerStatus)Enum.Parse(typeof(enmServiceControllerStatus), ((int)ctl.Status).ToString());
            }
            return ServiceState;
        }

        protected virtual void SetSeverityToWorker(BaseWorker c, int Severity)
        {
            if (Severity <= 0) // restore previous severity
            {
                if (c.Cfg.SavedLogSeverity > 0)
                {
                    c.Cfg.LogSeverity = (enmLogSeverity)c.Cfg.SavedLogSeverity;
                    c.Cfg.SavedLogSeverity = -1;
                }
            }
            else // set new severity
            {
                if (c.Cfg.SavedLogSeverity <= 0) // save configured severity
                    c.Cfg.SavedLogSeverity = (int)c.Cfg.LogSeverity;
                c.Cfg.LogSeverity = (enmLogSeverity)Severity;
            }
        }

        protected virtual void ReadAppConfig()
        {
            Config.ConfigFilePath = Config.GetAppSettingsValue<string>("ConfigFilePath", "").Trim();

            if (string.IsNullOrEmpty(Config.ConfigFilePath))
                Config.ConfigFilePath = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "App_Data");

            if (!Config.ConfigFilePath.EndsWith("\\")) Config.ConfigFilePath += "\\";

            LogSeverity = (enmLogSeverity)Enum.Parse(typeof(enmLogSeverity), Config.GetAppSettingsValue<string>("LogSeverityLevel", "Default"), true);
            //   useFileWatcher = Config.GetAppSettingsValue<bool>("UseFileWatcher", false);
            wcfHostURL = Config.GetAppSettingsValue<string>("WCFHostUrl", "");

            NotificationSystemSubjectName = Config.GetAppSettingsValue<string>("NotificationSystemSubjectName", "Not Specified type of the System");
        }

        protected virtual void WriteSysLog(string sender, string message, EventLogEntryType et, bool suppressEmail)
        {
            if (CanWriteToSysLog(et))
                mainevtLog.WriteEntry(sender + " > " + message, et);

            if (!suppressEmail)
            {
                enmLogSeverity sv;
                switch (et)
                {
                    case EventLogEntryType.Error:
                        sv = enmLogSeverity.Default;
                        break;
                    case EventLogEntryType.Information:
                        sv = enmLogSeverity.Info;
                        break;
                    case EventLogEntryType.Warning:
                        sv = enmLogSeverity.Warning;
                        break;
                    default:
                        sv = enmLogSeverity.None;
                        break;
                }

                if (sv != enmLogSeverity.None)
                    SendMail(NotificationSystemSubjectName + string.Format(" Service Log [{0}]", et), string.Format("<b>{0}</b><br/>{1}", sender, message), sv, "", "");
            }
        }

        protected virtual bool CanWriteToSysLog(EventLogEntryType et)
        {
            switch (et)
            {
                case EventLogEntryType.Error:
                case EventLogEntryType.Warning:
                    return true;
                case EventLogEntryType.Information:
                    return ((LogSeverity & enmLogSeverity.Info) > 0);
                default:
                    return false;
            }
        }


        protected BaseWorker GetWorkerStructure(Type type)
        {
            string shortType = type.FullName.Split(',')[0].Trim();

            return workers.FirstOrDefault(u => string.Compare(u.Cfg.InstanceType, shortType, StringComparison.OrdinalIgnoreCase) == 0);
        }

        public BaseWorker GetWorkerStructure(string uname)
        {
            return workers.FirstOrDefault(u => string.Compare(u.UniqueName, uname, StringComparison.OrdinalIgnoreCase) == 0);
        }

        protected string GetStateFilePath(BaseWorker ws)
        {

            string path = Path.Combine(Config.ConfigFilePath, "States");

            return Path.Combine(path, ws.Cfg.Name) + ".xml";
        }

        protected BaseWorker StartSingleWorker(WorkerConfig wcong, bool srvStart, bool forceFreeze)
        {
            BaseWorker ws = null;

            if (wcong.InitializationType == wcType.Predefined)
            {
                throw new NotImplementedException("InitializationType - Predefined");
            }
            else if (wcong.InitializationType == wcType.ByInstanceType)
            {

                wcong.RepeatableProcess = true;

                try
                {
                    ws = (BaseWorker)Types.CreateInstance(Utils.EntryAssemblyEName, wcong.InstanceType, new object[] { wcong });
                }
                catch (Exception ex)
                {
                    string msg = string.Format("Attempt to start Worker [{1}] (reason: {0}) is failed: {2}",
                                                srvStart ? "Service is starting" : "By direction",
                                                ws.Cfg.Name,
                                                Helpers.ExceptionToString(ex));

                    WriteSysLog(this.ServiceName + ".StartWorker", msg, EventLogEntryType.Error, false);
                    return null;
                }
            }


            string wstatePath = GetStateFilePath(ws);

            if (File.Exists(wstatePath))
            {
                try
                {
                    WorkerStates sts = XmlManager.DeserializeFromFile<WorkerStates>(wstatePath);
                    ws.SetupStates(sts);
                    ws.ClearState(ws.State);
                }
                catch
                {
                    ws.SetupStates();
                }
            }

            ws.State.BaseConfigurationLoadedDate = DateTime.Now;

            ws.WorkerSupportsCancellation = true;

            ws.OnComplete += new EventHandler(worker_OnComplete);
            ws.OnError += new BaseWorker.OnErrorEventHandler(worker_OnError);
            ws.OnStart += new EventHandler(worker_OnStart);
            ws.OnWorkerInfo += new BaseWorker.OnWorkerInfoEventHandler(worker_OnWorkerInfo);
            ws.OnWorkerWarning += new BaseWorker.OnWorkerWarningEventHandler(worker_OnWorkerWarning);
            ws.OnDebug += new BaseWorker.OnWorkerDebugEventHandler(worker_OnDebug);
            ws.OnDetails += new BaseWorker.OnWorkerDetailsEventHandler(worker_OnDetails);
            ws.OnGetWorkerStru += new BaseWorker.WorkerStruEventHandler(worker_OnGetWorkerStru);
            ws.OnSendSMTP += worker_OnSendSMTP;
            ws.OnSendSMTPWithAttachment += worker_OnSendSMTPWithAttachment;
            ws.OnSaveState += worker_OnSaveState;
            ws.OnWorkerState += worker_OnWorkerState;
            ws.DoWork += new DoWorkEventHandler(ws.RunTask);
            ws.WorkerCompleteRun += worker_WorkerCompleteRun;

            workers.Add(ws);

            ws.State.SetInHoldState((forceFreeze ? true : ws.Cfg.ExecuteCondition.StartFreezed));
            ws.RunWorkerAsync();

            WriteSysLog(this.ServiceName + ".StartWorker",
                string.Format("{0} is started. Reason: {1}", ws.Cfg.Name, srvStart ? "Service is starting" : "By direction"),
                         EventLogEntryType.Information, srvStart);


            return ws;
        }


        protected virtual string WorkerConfigFile
        {
            get { return Path.Combine(Config.ConfigFilePath, "WokerConfig.xml"); }
        }

        protected virtual WorkerConfigList<WorkerConfig> GetWorkerConfigs()
        {
            string md5;
            WorkerConfigList<WorkerConfig> wo = XmlManager.DeserializeFromFile<WorkerConfigList<WorkerConfig>>(WorkerConfigFile, out md5);
            wo.MD5 = md5;
            wo.CalcConfigsMD5();

            return wo;
        }

        protected void StartWorkers(bool svcstart)
        {

            if (!IsWorkersStopped)
                return;

            workersConfigList = GetWorkerConfigs();

            if (Config.GetAppSettingsValue<bool>("WriteTestConfig", false))
                XmlManager.SerializeToFile(Config.ConfigFilePath + "WokerConfigTest.xml", workersConfigList);

            workers.Clear();

            for (int l = 0; l < workersConfigList.Configs.Count; l++)
            {
                if (workersConfigList.Configs[l].Enabled)
                    StartSingleWorker(workersConfigList.Configs[l], svcstart, false);
            }
        }

        public void StopWorkers(bool shootdown)
        {
            if (!IsWorkersStopped)
            {
                for (int l = workers.Count - 1; l >= 0; l--)
                {

                    if (workers[l] != null && workers[l].State != null && !workers[l].State.Disposed)
                    {
                        workers[l].SetStopState();
                        StopSingleWorker(workers[l], shootdown, "");
                    }
                }
            }
        }

        protected void SaveWorkerState(BaseWorker ws)
        {

            string fpath = GetStateFilePath(ws);
            string path = Path.GetDirectoryName(fpath);

            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            XmlManager.SerializeToFile(fpath, ws.States);
        }

        protected void StopSingleWorker(BaseWorker ws, bool shutdown, string reason, EventLogEntryType logtype = EventLogEntryType.Information)
        {
            if (!ws.State.InStopState) ws.SetStopState();

            int counter = 0;
            while (ws.State.InProcess && counter <= WaitForThreadStop)
            {
                Thread.Sleep(200);
                counter += 200;
                if ((counter % 1000) == 0 && shutdown && !Environment.UserInteractive)
                    this.RequestAdditionalTime(1300);
            }

            ws.CancelAsync();

            counter = 0;
            while (!ws.WorkCompleted && counter <= WaitForThreadStop)
            {
                Thread.Sleep(200);
                counter += 200;
                if ((counter % 1000) == 0 && shutdown && !Environment.UserInteractive)
                    this.RequestAdditionalTime(1300);
            }

            string wrkName = ws.Cfg.Name;

            ws.Dispose();
            workers.Remove(ws);

            if (string.IsNullOrWhiteSpace(reason))
                reason = (shutdown ? "Service is shutting down" : "By direction");

            WriteSysLog(this.ServiceName + ".StopWorker", string.Format("{0} is stopped. Reason: {1}", wrkName, reason), logtype, shutdown);

        }

        protected void LoadWCF()
        {
            IRequestHandler handler = new RequestHandler(this);
            wcfHost = new WcfWrapper().CreateServer(handler, wcfHostURL);
            WriteSysLog(this.ServiceName + ".LoadWCF", string.Format("WCF Host [{0}] created. No actions required", wcfHostURL), EventLogEntryType.Warning, true);
        }

        protected void UnLoadWCF()
        {
            if (wcfHost != null)
            {
                wcfHost.Dispose();
                wcfHost = null;
                GC.Collect();
                GC.WaitForPendingFinalizers();
                WriteSysLog(this.ServiceName + ".UnLoadWCF", string.Format("WCF Host [{0}] was stopped. No actions required", wcfHostURL), EventLogEntryType.Warning, true);
            }
        }


        #endregion


        #region Service Actions

        public void ReloadAll()
        {

            WriteSysLog(this.ServiceName + ".ReloadAll", "Reloading environment by Remote request was initiated", EventLogEntryType.Warning, false);

            try
            {

                WriteSysLog(this.ServiceName + ".ReloadAll", "Stopping workers by Remote request", EventLogEntryType.Warning, true);

                StopWorkers(false);

                WriteSysLog(this.ServiceName + ".ReloadAll", "Stopping WCF Host by Remote request", EventLogEntryType.Warning, true);

                UnLoadWCF();

                WriteSysLog(this.ServiceName + ".ReloadAll", "Load configuration by Remote request", EventLogEntryType.Warning, true);

                LoadInitConfig();
                ReadAppConfig();

                WriteSysLog(this.ServiceName + ".ReloadAll", "Starting workers by Remote request", EventLogEntryType.Warning, true);

                StartWorkers(false);

                WriteSysLog(this.ServiceName + ".ReloadAll", "Starting WCF Host by Remote request", EventLogEntryType.Warning, true);

                LoadWCF();

                WriteSysLog(this.ServiceName + ".ReloadAll", "Reloading environment by Remote request was finished", EventLogEntryType.Warning, false);

                GetStatus();

            }
            catch (Exception ex)
            {
                WriteSysLog(this.ServiceName + ".ReloadAll", string.Concat(Helpers.ExceptionToString(ex), " : ", Config.ConfigFilePath),
                            EventLogEntryType.Error, false);

                ExitCode = ex.GetWin32Code();
                Stop();

            }

        }


        protected override void OnStart(string[] args)
        {

            try
            {

                ReadAppConfig();

                GetStatus();

                WriteSysLog(this.ServiceName + ".OnStart", string.Concat("Status: ",
                    Enum.GetName(typeof(ServiceControllerStatus), ServiceState)), EventLogEntryType.Information, true);

                StartWorkers(true);

                /*      if (useFileWatcher)
                      {
                          cfgChangeWatcher = new FileWatcher(WorkerConfigFile);
                          cfgChangeWatcher.OnFileChanged += CfgChangeWatcher_OnFileChanged;
                          cfgChangeWatcher.Start();
                      }*/

                LoadWCF();

                WriteSysLog(this.ServiceName + ".OnStart", "Internal operations are started", EventLogEntryType.Information, true);

                GetStatus();

            }
            catch (Exception ex)
            {
                WriteSysLog(this.ServiceName + ".OnStart", string.Concat(Helpers.ExceptionToString(ex), " : ", Config.ConfigFilePath),
                            EventLogEntryType.Error, false);

                ExitCode = ex.GetWin32Code();
                Stop();
            }

        }

        /*   private void CfgChangeWatcher_OnFileChanged(object sender, FileSystemEventArgs e)
           {
               TryReloadWorkerConfig("[Configuration File Watcher]", -1);
           }*/

        protected override void OnStop()
        {

            try
            {
                WriteSysLog(this.ServiceName + ".OnStop", "Stopping the service.", EventLogEntryType.Information, true);

                /*     if (useFileWatcher && cfgChangeWatcher != null)
                     {
                         cfgChangeWatcher.Dispose();
                         cfgChangeWatcher = null;
                     }*/

                GetStatus();

                StopWorkers(true);

                UnLoadWCF();

                WriteSysLog(this.ServiceName + ".OnStop", "Internal operations are stopped", EventLogEntryType.Information, true);

            }
            catch (Exception ex)
            {
                WriteSysLog(this.ServiceName + ".OnStop", Helpers.ExceptionToString(ex), EventLogEntryType.Error, false);

                ExitCode = ex.GetWin32Code();
            }
            finally
            {
                if (ExitCode != 0)
                {
                    Environment.Exit(ExitCode);
                }
            }

        }


        #endregion

    }
}
