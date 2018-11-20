using BaseWindowsService.Workers;
using NSHelpers.Storage;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.ServiceProcess;
using WindowsSvcMessage;
using System;
using System.Collections.Generic;

namespace BaseWindowsService
{
    public partial class BaseService : ServiceBase
    {

        public WorkerConfigDataList GetWorkers()
        {
            WorkerConfigDataList dl = new WorkerConfigDataList();
            for (int i = 0; i < workers.Count; i++)
            {
                WorkerConfig wc = workers[i].Cfg;
                WorkerConfigData wd = new WorkerConfigData()
                {
                    WorkerName = wc.Name,
                    InstanceType = wc.InstanceType,
                    AdditionalConfigurationFile = wc.AdditionalConfigurationFile,
                    AdditionalConfigurationFileType = wc.AdditionalConfigurationFileType
                };
                dl.Items.Add(wd);
            }

            return dl;
        }

        public WorkerStateDataList GetWorkersState()
        {
            WorkerStateDataList dl = new WorkerStateDataList();
            for (int i = 0; i < workers.Count; i++)
            {
                dl.Items.Add(FillWorkerState(workers[i]));
            }

            return dl;
        }

        public WorkerStateData GetWorkerState(string uname)
        {
            BaseWorker bw = GetWorkerStructure(uname);
            return FillWorkerState(bw);
        }

        public bool SetInHoldState(string uname, bool state)
        {
            BaseWorker bw = GetWorkerStructure(uname);

            if (bw == null)
                return false;

            bw.State.InHoldState = state;

            return true;
        }

        public int ReloadWorkersConfig(bool base_cfg, bool external_cfg)
        {
            return TryReloadWorkerConfig("Remote Command", base_cfg, external_cfg);
        }

        public int SetRunImmediatelly(string uname)
        {

            BaseWorker bw = GetWorkerStructure(uname);

            if (bw == null)
                return -1;

            if (!bw.SetRunImmediatelly())
                return 0;

            return 1;
        }

        public int ReloadWorker(string uname)
        {
            BaseWorker bw = GetWorkerStructure(uname);

            if (bw == null)
                return -1;

            try
            {
                WorkerConfig wc = bw.Cfg;

                StopSingleWorker(bw, false, "Directive Command");
                StartSingleWorker(wc, false, false);
            }
            catch (Exception ex)
            {
                WriteSysLog(this.ServiceName + ".ReloadWorker", string.Concat(Helpers.ExceptionToString(ex), " : ", Config.ConfigFilePath),
                            EventLogEntryType.Error, false);
                return 0;
            }
            return 1;
        }

        public List<ErrorHistoryItem> GetErrorHistory(string uname)
        {
            BaseWorker bw = GetWorkerStructure(uname);

            if (bw == null)
                return null;

            return bw.State.LastError.OrderByDescending(v=>v.TimeStamp).Select(u => new ErrorHistoryItem() { TimeStamp = u.TimeStamp, Message = u.Message }).ToList();
        }

        #region Private

        private WorkerStateData FillWorkerState(BaseWorker bw)
        {
            WorkerState wc = bw.State;
            WorkerStateData wd = new WorkerStateData()
            {
                UniqueName = bw.UniqueName,
                Name = wc.Name,
                CurrentProcess_Name = wc.CurrentProcessName,
                InStopState = wc.InStopState,
                InProcess = wc.InProcess,
                BatchMode = wc.InBatchMode,
                InHold = wc.InHoldState,
                InInternalProcess = wc.InInternalProcess,
                ExitedWithError = wc.ExitedWithError,
                InUnitsProcess = wc.InUnitsProcess,
                AttemptsMade = wc.AttemptsMade,
                IsDefaultTimerInterval = wc.IsDefaultTimerInterval,
                ActionCancelled = wc.ActionCancelled,
                Execute_Request_EndDate = bw.States.Items.Max(u => u.EndRequestExecuteDate),
                Execute_Request_StartDate = bw.States.Items.Max(u => u.StartRequestExecuteDate),
                Action_Last_StartDate = bw.States.Items.Max(u => u.LastStartDate),
                Action_Last_EndDate = bw.States.Items.Max(u => u.LastEndDate),
                BaseConfiguration_LoadedDate = wc.BaseConfigurationLoadedDate,
                ExternalConfiguration_LoadedDate = wc.ExternalConfigurationLoadedDate,
                Started_Date = bw.StartedDate,
                ErrorsCount = wc.LastError.Count,
                ExternalTasksCount = bw.ExternalTaskCommands == null ? 0 : bw.ExternalTaskCommands.Count,
                UnitsInProcessCount = bw.UnitsInProcessCount()
            };

            foreach(BaseWorkerUnit bi in bw.Units)
            {
                UnitState us = new UnitState(bi.State.InProcess)
                {
                    CurrentProcess_Name = bi.UniqueName
                };

                foreach(KeyValuePair<string,object> kp in wc.VirtualFields)
                {
                    us.VirtualFields.Add(new KeyValuePair<string, object>(kp.Key, kp.Value));
                }

                wd.Units.Add(us);

            }
            wd.UnitsCount = wd.Units.Count;

            return wd;

        }


        private int TryReloadWorkerConfig(string context, bool base_cfg, bool external_cfg)
        {

            if (!base_cfg && !external_cfg)
                return -2;

            if (base_cfg && !FileUtils.TryOpenFile(WorkerConfigFile, 3, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                return -1;
            }

            WorkerConfigList<WorkerConfig> wcl_changed = GetWorkerConfigs();

            if (string.CompareOrdinal(wcl_changed.MD5, workersConfigList.MD5) == 0 && !external_cfg)
                return 0;

            BaseWorker bw;
            bool configApplied_base = false;
            bool configApplied_external = false;
            wcConfigLoadType reload_type = wcConfigLoadType.None;

            if (base_cfg)
                reload_type = wcConfigLoadType.Base;
            if (external_cfg)
                reload_type |= wcConfigLoadType.External;

            bool have_base_part = ((reload_type & wcConfigLoadType.Base) > 0);
            bool have_external_part = ((reload_type & wcConfigLoadType.External) > 0);

            lock (objWorkerConfig_Lock)
            {
                for (int i = workers.Count - 1; i >= 0; i--)
                {
                    bw = workers[i];
                    bw.Cfg.FoundDuringUpdate = false;
                    bw.CfgPendingToChange = null;
                    bw.ExternalCfgPendingToChange = null;

                    WorkerConfig wc = wcl_changed.Configs.FirstOrDefault(u => u.Equals(bw.Cfg));

                    if (wc == null)
                    {
                        WriteSysLog(context,
                                    string.Format("Configuration file was changed and new configuration was not found for the Worker [{0}]. The Worker are stopping.", bw.Cfg.Name)
                                    , EventLogEntryType.Error, false);

                        StopSingleWorker(bw, false, "Configuration file was changed and new configuration was not found", EventLogEntryType.Warning);
                        configApplied_base = true;
                        continue;
                    }

                    wc.FoundDuringUpdate = true;
                    wc.ReloadConfigType = wcConfigLoadType.None;

                    if (!wc.Enabled && have_base_part)
                    {
                        StopSingleWorker(bw, false, "Configurations file was changed and Worker was disabled", EventLogEntryType.Warning);
                        configApplied_base = true;
                        continue;
                    }

                    if (have_external_part)
                    {
                        if (bw.HaveExternalConfig)
                        {
                            if (!FileUtils.TryOpenFile(bw.ExternalConfigFilePath, 3, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                                return -2;

                            IStepsConfig stp_cfg = bw.LoadExternalConfigFile();
                            if (string.CompareOrdinal(bw.ExternalTaskCommands.MD5, stp_cfg.MD5) != 0)
                            {
                                bw.ExternalCfgPendingToChange = stp_cfg;
                                bw.CfgPendingToChange = wc;
                                configApplied_external = true;
                                wc.ReloadConfigType = wcConfigLoadType.External;
                            }
                        }
                    }

                    if (have_base_part && (string.CompareOrdinal(wc.MD5, bw.Cfg.MD5) != 0))
                    {
                        wc.ReloadConfigType |= wcConfigLoadType.Base;
                        bw.CfgPendingToChange = wc;
                        configApplied_base = true;
                    }
                }


                WorkerConfig[] wc_ar = wcl_changed.Configs.Where(u => !u.FoundDuringUpdate && u.Enabled).ToArray();

                if (wc_ar.Length > 0)
                {
                    for (int i = 0; i < wc_ar.Length; i++)
                    {
                        WriteSysLog(context,
                                    string.Format("Configuration file was changed and new configuration was found for the Worker [{0}]. The Worker are starting.", wc_ar[i].Name)
                                    , EventLogEntryType.Warning, false);
                        StartSingleWorker(wc_ar[i], false, false);
                        configApplied_base = true;
                    }
                }


                if (have_base_part && (configApplied_base))
                {
                    wcl_changed.Configs.ForEach(u => u.FoundDuringUpdate = false);
                    workersConfigList = wcl_changed;
                }
            }

            return (configApplied_base || configApplied_external) ? 1 : 0;

        }

        public void RequestHandlerError(Exception ex)
        {
            WriteSysLog(this.ServiceName + ".Communications", Helpers.ExceptionToString(ex), EventLogEntryType.Error, false);
        }

        #endregion


    }
}
