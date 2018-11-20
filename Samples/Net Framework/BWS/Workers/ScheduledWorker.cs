using NSHelpers;
using System;
using System.Linq;

namespace BaseWindowsService.Workers
{
    public class ScheduledWorker : BaseWorker
    {

        public ScheduledWorker(WorkerConfig cfg)
        {
            Init(cfg);
        }


        protected bool RequestExecuted;

        public override void ResetAll(bool suppressEvents = false)
        {
            base.ResetAll(suppressEvents);

            ClearState(State);

            if (!suppressEvents)
                State.InProcess = false;
            else
                State.SetInProcess(false);

        }

        protected static int CheckExecuteConditions(ExecuteConditions ecfg, WorkerState commonstate)
        {
            return CheckExecuteConditions(ecfg, commonstate, commonstate);
        }

        protected static int CheckExecuteConditions(ExecuteConditions ecfg, WorkerState unitState, WorkerState basestate)
        {

            if (!ecfg.Runimmediately && unitState.RunImmediately_Directive != enmUpdateCommandState.InProgress && basestate.RunImmediately_Directive != enmUpdateCommandState.InProgress)
            {
                if (ecfg.ExecIntervalType == wcExecIntervalType.Weekly)
                {
                    DateTime currentDate = DateTime.Now;
                    TimeSpan currtime = DateTime.Now.TimeOfDay;
                    int currWeekDay = (int)currentDate.DayOfWeek;
                    //allow additional seconds from starting point
                    TimeSpan topCfgBorder = new DateTime(currentDate.Year, currentDate.Month, currentDate.Day, ecfg.ExecInTime.Hours, ecfg.ExecInTime.Minutes, ecfg.ExecInTime.Seconds)
                        .AddSeconds(ecfg.ExecInTimeAdditionalSeconds).TimeOfDay;

                    DateTime startDate = unitState.LastStartDate;
                    DateTime endDate = unitState.LastEndDate;

                    if (startDate.Date > currentDate.Date)
                    {
                        startDate = currentDate;
                        endDate = currentDate;
                    }

                    bool missedStart = false;
                    bool timeToStart = false;

                    if (ecfg.DaysToExecute.Contains(currWeekDay)) // today scheduled
                    {
                        if (currtime < ecfg.ExecInTime)  // it was scheduled for today but it is not a time to run
                            return 0;

                        if (startDate.Date == currentDate.Date && startDate.TimeOfDay >= ecfg.ExecInTime) // it was scheduled for today and was executed already
                            return 0;

                        timeToStart = (currtime >= ecfg.ExecInTime && currtime <= topCfgBorder); // it is time to start

                        if (!timeToStart && ecfg.ExecIfTimeMissed)
                        {
                            missedStart = (startDate.Date < currentDate.Date && currtime > topCfgBorder);  // scheduled for today but missed

                            if (!missedStart)
                                missedStart = (currtime > topCfgBorder && startDate.Date == currentDate.Date && startDate.TimeOfDay < ecfg.ExecInTime); // was executed today not in time

                        }
                    }
                    else if (ecfg.ExecIfTimeMissed)
                    {
                        if (startDate > DateTime.MinValue && startDate.Date < currentDate.Date)    // check on missed call
                        {
                            int daysDiff = (currentDate - startDate).Days;

                            if (daysDiff > 7)  // last run was more than week ago
                                missedStart = true;
                            else
                            {
                                int weeksDiff = -1;

                                /*    if (ecfg.DaysToExecute.Count == 1)
                                        weeksDiff = 7;
                                    else
                                    {*/
                                int nearestDayofWeek = -1;
                                if (ecfg.DaysToExecute.Any(u => u < currWeekDay))
                                    nearestDayofWeek = ecfg.DaysToExecute.Last(u => u < currWeekDay);
                                else if (ecfg.DaysToExecute.Any(u => u > currWeekDay))
                                    nearestDayofWeek = ecfg.DaysToExecute.Last(u => u > currWeekDay);

                                if (nearestDayofWeek >= 0)
                                    weeksDiff = currentDate.WeekDaysDiff(nearestDayofWeek, true);
                                // }

                                if (weeksDiff > 0)
                                {
                                    DateTime mustberunDate = currentDate.AddDays(0 - weeksDiff);

                                    if (mustberunDate.Date > endDate.Date)
                                        missedStart = true;
                                }
                            }
                        }
                    }

                    if (missedStart)
                        return 2;
                    else if (timeToStart)
                        return 3;
                    else
                        return 0;
                }
            }

            return 1;
        }

        protected virtual int ActionStarted()
        {
            return CheckExecuteConditions(Cfg.ExecuteCondition, State);
        }

        protected override void DoActions()
        {

            try
            {

                State.StartRequestExecuteDate = DateTime.Now;

                if (ActionStarted() == 0)
                    return;

                try
                {
                    if (AutomaticallyManageInternalProcess)
                    {
                        State.InInternalProcess = true;
                        State.SetLastStartDate(DateTime.Now);
                    }

                    State.CurrentProcessName = Cfg.Name;
                    ExecuteReturnValue = EMPTY_RETURN_VALUE;
                    ExecuteReturnTable = null;


                    RaiseOnStart();

                    RequestExecuted = ExecuteRequest();
                }
                catch (Exception e)
                {
                    RaiseOnError(e);
                }
                finally
                {

                    ActionFinished();

                    State.CurrentProcessName = "";

                    RaiseOnComplete();

                }
            }
            finally
            {
                State.EndRequestExecuteDate = DateTime.Now;
            }
        }

        protected virtual void ActionFinished()
        {

            if (AutomaticallyManageInternalProcess)
                State.InInternalProcess = false;

            if ((int)Cfg.ExecuteCondition.ExecIntervalType >= (int)wcExecIntervalType.Daily)
            {
                if (RequestExecuted && !State.ExitedWithError && !State.ActionCancelled && !BreakIfSystem)
                {

                    if (AutomaticallyManageInternalProcess)
                        State.SetLastEndDate(DateTime.Now);

                    RaiseOnSaveState();
                }
                else
                    States.RestoreLastDates();
            }

        }

        protected virtual bool ExecuteRequest()
        {
            return false;
        }





    }


}
