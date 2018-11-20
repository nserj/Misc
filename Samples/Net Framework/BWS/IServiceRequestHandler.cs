using WindowsSvcMessage;
using BaseWindowsService.Workers;
using System;
using System.Collections.Generic;

namespace BaseWindowsService
{
    public interface IServiceRequestHandler
    {

        WorkerConfigDataList GetWorkers();
        WorkerStateDataList GetWorkersState();
        WorkerStateData GetWorkerState(string uname);
        BaseWorker GetWorkerStructure(string uname);
        bool SetInHoldState(string uname, bool state);
        int ReloadWorkersConfig(bool base_cfg, bool external_cfg);
        void RequestHandlerError(Exception ex);
        int SetRunImmediatelly(string uname);
        int ReloadWorker(string uname);
        List<ErrorHistoryItem> GetErrorHistory(string uname);
        enmServiceControllerStatus GetStatus();
        void ReloadAll();
    }
}
