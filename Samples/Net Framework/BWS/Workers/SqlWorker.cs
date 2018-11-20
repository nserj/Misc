using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Windows.Forms;

namespace BaseWindowsService.Workers
{

    #region SQL Service State

    public class SWState : IDisposable
    {

        public SqlConnection cnn;
        public SqlDataAdapter da;

        private SqlCommand _cmd;
        public SqlCommand cmd
        {
            get { return _cmd; }
            set
            {
                if (_cmd != null)
                {
                    try
                    {
                        _cmd.Dispose();
                    }
                    catch { }
                }

                _cmd = value;
                if (_cmd != null)
                {
                    _IsCommandDisposed = false;
                    _cmd.Disposed += new EventHandler(_cmd_Disposed);
                }
            }
        }

        void _cmd_Disposed(object sender, EventArgs e)
        {
            _IsCommandDisposed = true;
        }

        private bool _IsCommandDisposed;
        public bool IsCommandDisposed
        {
            get { return (_cmd == null || _IsCommandDisposed); }
        }

        public SWState() { }
        public SWState(SqlConnection con, SqlCommand comd)
        {
            cnn = con;
            cmd = comd;
        }

        ~SWState()
        {
            Dispose(false);
        }

        #region Disposable

        private bool disposed;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (!this.disposed)
            {
                if (disposing)
                {
                    if (!IsCommandDisposed)
                    {
                        cmd.Cancel();
                        cmd.Dispose();
                    }

                    if (da != null)
                        da.Dispose();

                    if (cnn != null)
                        cnn.Dispose();
                }
            }
            disposed = true;
        }
    }
    #endregion

    #endregion

    public class SqlWorker : BaseWorker
    {

        public enum enmCmdExecuteType
        {
            NonQuery = 0,
            Reader = 1
        }


        protected SWState SQLObject = new SWState();
        protected EventWaitHandle _batchBlockerHndl = new EventWaitHandle(false, EventResetMode.AutoReset);
        protected bool sqlRequestHandled = true;

        protected Stack<long> progressRow = new Stack<long>();

        protected DataTable LastReceivedTaskData;

        protected enmCmdExecuteType CommandExecuteType = enmCmdExecuteType.NonQuery;

        public SqlWorker() { }

        public SqlWorker(WorkerConfig cfg)
        {
            Init(cfg);
        }

        #region Worker's area

  
        public override void ResetAll(bool suppressEvents = false)
        {
            base.ResetAll(suppressEvents);

            if (LastReceivedTaskData != null)
            {
                LastReceivedTaskData.Dispose();
                LastReceivedTaskData = null;
            }

            ClearBase();
            State.InBatchMode = false;

            if (!suppressEvents)
                State.InProcess = false;
            else
                State.SetInProcess(false);

        }

        protected void ClearBase()
        {
            progressRow.Clear();
            sqlRequestHandled = true;
            ClearState(State);
            State.InBatchMode = false;
            SQLObject.cnn?.Close();
        }


        protected override void DoDispose()
        {
            progressRow.Clear();
            if (SQLObject != null) SQLObject.Dispose();
            if (_batchBlockerHndl != null) _batchBlockerHndl.Dispose();
        }

        #endregion

        #region Command/Connection

        protected SqlCommand GetCommand(string expr)
        {
            return GetCommand(expr, CommandType.StoredProcedure);
        }

        protected SqlCommand GetCommand(string expr, CommandType ctype)
        {
            SqlCommand cmd = new SqlCommand(expr, SQLObject.cnn)
            {
                CommandTimeout = Cfg.SQLCommandCondition.CommandTimeOut,
                CommandType = ctype
            };
            return cmd;
        }

        protected bool OpenConnection(ref SqlConnection var, string connStr = "")
        {
            string cns = string.IsNullOrWhiteSpace(connStr) ? Cfg.SQLCommandCondition.ConnectionString : connStr;

            try
            {

                if (var == null)
                    var = new SqlConnection(cns);

                if (var.State != ConnectionState.Open)
                    var.Open();

                return true;
            }
            catch
            {
                try
                {
                    if (var != null)
                        var.Dispose();
                    var = new SqlConnection(cns);
                    var.Open();
                    return true;
                }
                catch (Exception ex2)
                {
                    Exception cex = ex2;

                    if (ex2.GetType().Equals(typeof(SqlException)))
                        cex = new ExtendedException(ex2, "",Helpers.ParseSQLConnection(var),null);

                    State.SetError(10, cex);
                    RaiseOnError(State);

                    if (var != null)
                        var.Dispose();

                    var = null;

                    return false;
                }

            }
        }

        protected virtual void CommandAddDynamicParameters(SqlCommand cmd) { }

        protected virtual void PrepareCommand()
        {

            SQLObject.cmd = GetCommand(Cfg.SQLCommandCondition.Command);
            SQLObject.cmd.Parameters.Clear();

            for (int l = 0; l < Cfg.SQLCommandCondition.CommandParameters.Count; l++)
            {
                SQLObject.cmd.Parameters.Add(new SqlParameter(Cfg.SQLCommandCondition.CommandParameters[l].Name, Cfg.SQLCommandCondition.CommandParameters[l].Value));
            }

            CommandAddDynamicParameters(SQLObject.cmd);

            SqlParameter oRetParam = new SqlParameter
            {
                Direction = ParameterDirection.ReturnValue,
                SqlDbType = SqlDbType.Int,
                ParameterName = "NewRowID"
            };
            SQLObject.cmd.Parameters.Add(oRetParam);
        }


        #endregion

        #region Main request

        protected virtual void ExecuteRequest()
        {
            ClearBase();

            if (!OpenConnection(ref SQLObject.cnn))
                return;

            RaiseOnStart();

            PrepareCommand();

            IAsyncResult result;

            if (CommandExecuteType == enmCmdExecuteType.NonQuery)
                result = SQLObject.cmd.BeginExecuteNonQuery(HandleCallback, null);
            else if (CommandExecuteType == enmCmdExecuteType.Reader)
                result = SQLObject.cmd.BeginExecuteReader(HandleCallback, null);
            else
                throw new NotImplementedException(string.Format("Execute Request. {0} ", CommandExecuteType));

            sqlRequestHandled = false;
        }

        protected override void DoActions()
        {
            try
            {
                if (AutomaticallyManageInternalProcess)
                    State.InInternalProcess = true;

                State.InSQLAction = true;

                ExecuteReturnValue = EMPTY_RETURN_VALUE;
                ExecuteReturnTable = null;

                State.StartRequestExecuteDate = DateTime.Now;
                State.CurrentProcessName = Cfg.Name;

                try
                {
                    ExecuteRequest();

                    if (!State.ExitedWithError)
                    {
                        while (State.InSQLAction)
                        {
                            Thread.Sleep(100);
                        }
                    }

                }
                catch (Exception ex)
                {
                    if (ex.GetType().Equals(typeof(SqlException)))
                        RaiseOnError(new ExtendedException(ex, "", Helpers.ParseSQLCommand(SQLObject?.cmd), null));
                    else
                        RaiseOnError(ex);

                    sqlRequestHandled = true;
                    State.InSQLAction = false;
                }
            }
            finally
            {

                State.EndRequestExecuteDate = DateTime.Now;
                State.CurrentProcessName = "";

                if (AutomaticallyManageInternalProcess)
                    State.InInternalProcess = false;

            }
        }


        protected virtual bool CallBackPostProcess(bool havedata) { return false; }

        protected virtual void ProcessHandleCallback(IAsyncResult result)
        {
            if (!SQLObject.IsCommandDisposed)
            {

                bool havedata = true;

                if (CommandExecuteType == enmCmdExecuteType.NonQuery)
                {
                    SQLObject.cmd.EndExecuteNonQuery(result);
                    ExecuteReturnValue = Convert.ToInt32(SQLObject.cmd.Parameters["NewRowID"].Value);
                }
                else if (CommandExecuteType == enmCmdExecuteType.Reader)
                {

                    PrepareTableResult(SQLObject, result);
                    havedata = (ExecuteReturnTable != null && ExecuteReturnTable.Rows.Count > 0);
                }


                if (!BreakIfSystem)
                {
                    DateTime dt = DateTime.Now;
                    if (CallBackPostProcess(havedata))
                    {
                        State.SetLastStartDate(dt);
                        State.SetLastEndDate(DateTime.Now);
                    }
                }
            }
        }

        protected virtual void HandleCallback(IAsyncResult result)
        {
            try
            {
                ProcessHandleCallback(result);
            }
            catch (Exception ex)
            {
                if (ex.GetType().Equals(typeof(SqlException)))
                    RaiseOnError(new ExtendedException(ex, "", Helpers.ParseSQLCommand(SQLObject?.cmd), null));
                else
                    RaiseOnError(ex);
            }
            finally
            {

                if (result != null && result.AsyncWaitHandle != null)
                    result.AsyncWaitHandle.Close();

                sqlRequestHandled = true;

                RaiseOnComplete();

                if (!State.ExitedWithError && !State.Disposed && !BreakIfSystem && State.InBatchMode)
                {
                    if (Cfg.ExecuteCondition.ThreadSleepTimeoutBatchMode > 0 && !State.Disposed)
                        _batchBlockerHndl.WaitOne(Cfg.ExecuteCondition.ThreadSleepTimeoutBatchMode, true);

                    MethodInvoker md = new MethodInvoker(ExecuteRequest);
                    md.BeginInvoke(null, null);
                }
                else
                    State.InSQLAction = false;
            }
        }

        protected virtual void PrepareTableResult(SWState SQLObject, IAsyncResult result)
        {
            ExecuteReturnTable = null;
            try
            {
                using (SqlDataReader rd = SQLObject.cmd.EndExecuteReader(result))
                {

                    if (rd != null)
                    {
                        DataTable tbsch = rd.GetSchemaTable();
                        DataTable tb = new DataTable();
                        DataColumn cl;

                        foreach (DataRow dr in tbsch.Rows)
                        {
                            cl = new DataColumn(dr["ColumnName"].ToString(), (Type)dr["DataType"]);
                            tb.Columns.Add(cl);
                        }

                        object[] vals = new object[rd.FieldCount];

                        while (rd.Read())
                        {
                            rd.GetValues(vals);
                            tb.LoadDataRow(vals, true);
                        }

                        ExecuteReturnTable = tb;

                    }
                }
            }
            catch (Exception ex)
            {
                Exception e = new Exception("ProcessHandleCallback: Collect Result table.", ex);
                throw e;

            }
        }

        #endregion

        #region Support area

        public bool AsyncExecuteNonQuery(SqlConnection conn, string strcommand, CommandType cmdtype, params object[] prm)
        {

            SqlParameter[] spm= null;

            if (prm != null && prm.Length > 0 && (prm.Length % 2) == 0)
            {
                spm = new SqlParameter[prm.Length / 2];
                for (int l = 0; l < prm.Length - 1; l = l + 2)
                {
                    spm[l]= new SqlParameter(prm[l].ToString(), prm[l + 1]);
                }
            }

           return AsyncExecuteNonQuery(conn, strcommand, cmdtype, spm);
        }

        public bool AsyncExecuteNonQuery(SqlConnection conn, string strcommand, CommandType cmdtype, params SqlParameter[] prm)
        {
            bool canceled = false;

            if (conn.State != ConnectionState.Open)
                conn.Open();

            using (SqlCommand command = new SqlCommand(strcommand, conn))
            {
                command.CommandType = cmdtype;
                command.CommandTimeout = Cfg.SQLCommandCondition.CommandTimeOut;
                if (prm != null && prm.Length > 0)
                    command.Parameters.AddRange(prm);

                IAsyncResult result = null;
                try
                {
                    result = command.BeginExecuteNonQuery();

                    while (!result.IsCompleted)
                    {
                        if (State.ActionCancelled || ImmediatelyStop)
                        {
                            command.Cancel();
                            canceled = true;
                            break;
                        }
                        Thread.Sleep(100);
                    }

                }
                finally
                {
                    if (result != null)
                    {
                        try
                        {
                            command.EndExecuteNonQuery(result);
                        }
                        catch (SqlException ex)
                        {
                            switch (ex.ErrorCode)
                            {
                                case -2146232060:
                                    if (!ex.Message.Contains(" отменена ") && !ex.Message.Contains(" cancelled "))
                                        throw;
                                    break;
                                default:
                                    throw;
                            }
                        }
                    }
                }
            }

            return canceled;
        }

        #endregion

    }


}
