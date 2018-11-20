using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.Threading.Tasks;
using WndowsSvcRequest;
using WindowsSvcMessage;

namespace BaseWindowsService.WCFCommunication
{
    [ServiceBehavior(
         ConcurrencyMode = ConcurrencyMode.Multiple,
         InstanceContextMode = InstanceContextMode.Single,
         IncludeExceptionDetailInFaults = true)]
    public class RequestHandler : IRequestHandler
    {
        private readonly Dictionary<long, AcceptedRequest> _requests = new Dictionary<long, AcceptedRequest>();
        private long _lastRequestId; // = 0
        private long _lastRevision; // = 0

        private IServiceRequestHandler _serviceRequestHandler;

        public RequestHandler(IServiceRequestHandler srh)
        {
            _serviceRequestHandler = srh;
        }

        public AcceptedRequest CreateRequest(Request rawRequest)
        {
            int delay;

            AcceptedRequest request = CreateAcceptedRequest(rawRequest, out delay);

            Task.Delay(delay).ContinueWith((task, state) => CompleteRequest(request), null);

            return request;
        }

        private AcceptedRequest CreateAcceptedRequest(Request rawRequest, out int delay)
        {
            if (rawRequest == null) throw new ArgumentException(nameof(rawRequest));

            delay = rawRequest.RequestedProcessingTimeMs;

            if (delay < 0)
            {
                throw new ArgumentException("RequestProcessingTimeMs must not be negative");
            }

            AcceptedRequest request;
            long revision;
            long id;

            lock (_requests)
            {

                var now = DateTime.UtcNow;

                id = ++_lastRequestId;
                revision = ++_lastRevision;

                request = new AcceptedRequest
                {
                    Id = id,
                    Revision = revision,
                    Input = rawRequest.Input,
                    RequestedProcessingTimeMs = delay,
                    CreatedOn = now,
                    ScheduledCompletionTime = now.AddMilliseconds(delay)
                };

                _requests[id] = request;

            }

            return request;
        }

        public bool ClearAllRequests()
        {
            lock (_requests)
            {
                _requests.Clear();
                _lastRevision = 0;
                _lastRequestId = 0;
            }

            return true;
        }

        public bool DeleteRequests(long id)
        {
            lock (_requests)
            {
                AcceptedRequest result;
                if (!_requests.TryGetValue(id, out result)) return false;

                _requests.Remove(id);

                return true;
            }
        }

        public AcceptedRequest GetRequest(long id)
        {
            lock (_requests)
            {
                AcceptedRequest result;
                if (!_requests.TryGetValue(id, out result)) return null;
                return result;
            }
        }

        public RequestList GetAllRequests(long sinceRevision)
        {
            lock (_requests)
            {
                return new RequestList
                {
                    Revision = _lastRevision,
                    Requests = _requests.Values
                        .Where(r => r.Revision > sinceRevision)
                        .OrderByDescending(r => r.ActualCompletionTime ?? r.ScheduledCompletionTime)
                        .ToArray()
                };
            }
        }

        private AcceptedRequest CompleteRequest(AcceptedRequest request)
        {
            var now = DateTime.UtcNow;

            Message cm = Message.Parse(request.Input);

            CommandParameter prm;
            CommandParameters prm_arr;
            Workers.BaseWorker bw;
            AnswerParameter _answer = new AnswerParameter(true);
            int ret;

            Action delayedTask = null;

            try
            {
                switch (cm.Command)
                {
                    case enmCommand.GetWorkerConfigList:
                        _answer.ValueData = _serviceRequestHandler.GetWorkers();
                        break;
                    case enmCommand.GetWorkerStateList:
                        _answer.ValueData = _serviceRequestHandler.GetWorkersState();
                        break;
                    case enmCommand.GetWorkerState:
                        prm = (CommandParameter)cm.InputData;
                        _answer.ValueData = _serviceRequestHandler.GetWorkerState(prm.GetValue<string>());
                        break;
                    case enmCommand.GetErrorHistory:
                        prm = (CommandParameter)cm.InputData;
                        _answer.ValueData = _serviceRequestHandler.GetErrorHistory(prm.GetValue<string>());
                        break;
                    case enmCommand.SetWorkerInHoldState:

                        prm_arr = (CommandParameters)cm.InputData;
                        bw = _serviceRequestHandler.GetWorkerStructure(prm_arr.Items[0].GetValue<string>());
                        if (bw != null)
                        {
                            bw.SetInHoldState(prm_arr.Items[1].GetValue<bool>());
                        }
                        else
                        {
                            _answer.ValueData = false;
                            _answer.Message = "Requested Worker was not found";
                        }

                        break;

                    case enmCommand.ReloadWorkersConfig:

                        prm_arr = (CommandParameters)cm.InputData;
                        int res = _serviceRequestHandler.ReloadWorkersConfig(prm_arr.Items[0].GetValue<bool>(), prm_arr.Items[1].GetValue<bool>());

                        if (res == 1 || res == 0)
                        {
                            _answer.Message = (res == 1 ? "" : "Configuration has had not been changed");
                        }
                        else
                        {
                            _answer.ValueData = false;
                            if (res == -2)
                                _answer.Message = "Incorrect command";
                            else if (res == -1)
                                _answer.Message = "Can not open configuration file";
                            else
                                _answer.Message = "Unknown result state";
                        }

                        break;

                    case enmCommand.RunImmediatelly:

                        prm = (CommandParameter)cm.InputData;
                        ret = _serviceRequestHandler.SetRunImmediatelly(prm.GetValue<string>());

                        switch(ret)
                        {
                            case -1:
                                _answer.ValueData = false;
                                _answer.Message = "Requested Worker was not found";
                                break;
                            case 0:
                                _answer.ValueData = false;
                                _answer.Message = "The request cannot be processed due the Worker's state";
                                break;
                        }

                        break;

                    case enmCommand.ReloadWorker:

                        prm = (CommandParameter)cm.InputData;
                        ret = _serviceRequestHandler.ReloadWorker(prm.GetValue<string>());

                        switch (ret)
                        {
                            case -1:
                                _answer.ValueData = false;
                                _answer.Message = "Requested Worker was not found";
                                break;
                            case 0:
                                _answer.ValueData = false;
                                _answer.Message = "Operation of Reload the Worker is failed";
                                break;
                        }

                        break;

                    case enmCommand.GetServiceState:
                        _answer.ValueData = _serviceRequestHandler.GetStatus();
                        break;

                    case enmCommand.ReloadAll:

                        delayedTask = new Action(() => ReloadServer());
                        _answer.ValueData = true;
                        _answer.Message = "Reload will be launched in 2 second.";

                        break;


                    default:
                        _answer.ValueData = false;
                        _answer.Message = "Unknown Command";
                        break;
                }
            }
            catch (Exception ex)
            {
                _serviceRequestHandler.RequestHandlerError(ex);
                _answer.ValueData = false;
                _answer.Message = Helpers.ExceptionToString(ex);
            }

            cm.OutputData = _answer;
         //   cm.PackData();

            lock (_requests)
            {
                request.Revision = ++_lastRevision;
                request.Output = cm.Pack();
                request.ActualCompletionTime = now;
                request.ActualProcessingTimeMs = (int)((now - request.CreatedOn).TotalMilliseconds + 0.5);
                request.IsCompleted = true;
            }


            if(delayedTask !=null)
            {
                Task.Delay(2000).ContinueWith((task)=> delayedTask.Invoke());
            }

            return request;


        }

        protected void ReloadServer()
        {
            _serviceRequestHandler.ReloadAll();
        }

        public long GetRequestQueueLength()
        {
            return _requests == null ? 0 : _requests.Count;
        }
    }
}
