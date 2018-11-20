using System;
using System.Data.SqlClient;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Runtime.Serialization;

namespace DP2SaaSMCS
{

    [Serializable]
    public class ExtendedException : Exception
    {
        public Exception BaseException { get; set; }
        public string Details { get; set; }

        public ExtendedException() { }
        public ExtendedException(Exception baseex, string message, string details, Exception innerexception):base(message,innerexception)
        {
            BaseException = baseex;
            Details = details;
        }

        public ExtendedException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }

    }

    public class Utils
    {

        public static string SafeTostring(object obj)
        {
            if (obj == null)
                return "NULL";
            return obj.ToString();
        }

        public static int GetLineNumber(Exception ex)
        {
            var lineNumber = 0;

            StackTrace trace = new StackTrace(ex, true);
            if (trace.FrameCount > 0)
            {
                lineNumber = trace.GetFrame(0).GetFileLineNumber();
            }

            if (lineNumber == 0)
            {
                const string lineSearch = ":line ";
                if (ex.StackTrace != null)
                {
                    var index = ex.StackTrace.LastIndexOf(lineSearch);
                    if (index != -1)
                    {
                        var lineNumberText = ex.StackTrace.Substring(index + lineSearch.Length);
                        int.TryParse(lineNumberText, out lineNumber);
                    }
                }
            }

            return lineNumber;
        }

        public static string ExceptionToStringForMessage(Exception ex)
        {
            StringBuilder ret = new StringBuilder(500);
            Exception lex = ex;

            while (lex != null)
            {
                if (lex.GetType().Equals(typeof(ExtendedException)))
                {
                    ExtendedException pe = (ExtendedException)lex;
                    ret.Append(ParseException(pe.BaseException,false));
                    ret.AppendFormat("=> DETAILS: => {0}", pe.Details);
                    lex = pe.BaseException.InnerException;
                }
                else
                {
                    ret.Append(ParseException(lex,false));
                    lex = lex.InnerException;
                }
            }
            return ret.ToString();
        }


        public static string ExceptionToString(Exception ex)
        {
            StringBuilder ret = new StringBuilder(500);
            Exception lex = ex;

            while (lex != null)
            {
                if (lex.GetType().Equals(typeof(ExtendedException)))
                {
                    ExtendedException pe = (ExtendedException)lex;
                    ret.Append(ParseException(pe.BaseException));
                    ret.AppendFormat("\nDETAILS:\n{0}", pe.Details);
                    lex = pe.BaseException.InnerException;
                }
                else
                {
                    ret.Append(ParseException(lex));
                    lex = lex.InnerException;
                }
            }
            return ret.ToString();
        }

        public static string ParseSQLConnection(SqlConnection  cnn)
        {
            if (cnn == null) return "";
            return string.Format("CONNECTION: {0}, State {1}", cnn.ConnectionString, cnn.State.ToString());
        }

        public static string ParseSQLCommand(SqlCommand cmd)
        {
            return ParseSQLCommand(cmd, true);
        }

        public static string ParseSQLCommand(SqlCommand cmd,bool useNewLine)
        {
            if (cmd==null)
                return "Command is null";

            string newLine = (useNewLine ? "\n" : " => ");

            StringBuilder s=new StringBuilder(500);  

            s.AppendFormat("COMMAND:{0}, Type: {1}, Timeout: {2} ",cmd.CommandText,cmd.CommandType.ToString() ,cmd.CommandTimeout);

            if (cmd.Connection != null)
                s.AppendFormat("{0}{1}", newLine, ParseSQLConnection(cmd.Connection)); 

            if (cmd.Parameters.Count > 0)
            {
                s.AppendFormat("{0}PARAMETERS:{0}",newLine);
                SqlParameter [] pa =new  SqlParameter[cmd.Parameters.Count];
                cmd.Parameters.CopyTo(pa,0);
                foreach (SqlParameter p in pa)
                {
                    s.AppendFormat("Name: {0}, Value: {1}, Type {2}{3}", 
                        p.ParameterName, (p.Value==null ? "" : p.Value.ToString()), p.SqlDbType.ToString(),newLine);     
                }
            }

            return s.ToString(); 
        }


        public static string ParseSQLException(SqlException ex)
        {
            return string.Format("Severity: {0}, Code: {1}, Number: {2}, Source: {3}, Procedure: {4}, Line: {5}, MESSAGE: {6}", ex.Class, ex.ErrorCode, ex.Number, ex.Source, ex.Procedure
                                                         , ex.LineNumber, ex.Message);
        }

        public static string ParseBaseException(Exception exception)
        {
            return ParseBaseException(exception, true);
        }

        public static string ParseBaseException(Exception exception, bool useNewLine)
        {
            string errorLine = "";

            string newLine = (useNewLine ? "\n" : "=>");

            StackTrace trace = new StackTrace(exception, true);

            errorLine = string.Concat("LINE: ", GetLineNumber(exception).ToString(), newLine);
            if (!string.IsNullOrEmpty(exception.Message)) errorLine = string.Concat(errorLine, "MESSAGE: ", exception.Message, newLine);
            if (!string.IsNullOrEmpty(exception.StackTrace)) errorLine = string.Concat(errorLine, "STACK TRACE: ", exception.StackTrace, newLine);
            if (!string.IsNullOrEmpty(exception.HelpLink)) errorLine = string.Concat(errorLine, "HELP LINK: ", exception.HelpLink, newLine);
            if (!string.IsNullOrEmpty(exception.Source)) errorLine = string.Concat(errorLine, "SOURCE: ", exception.Source, newLine);
            if (exception.TargetSite != null) errorLine = string.Concat(errorLine, "TARGET SITE: ", exception.TargetSite.ToString());

            return errorLine;
        }

        public static string ParseException(Exception exception)
        {
            return ParseException(exception, true);
        }

        public static string ParseException(Exception exception, bool useNewLine)
        {

            if (exception == null) return "";

            if (exception.GetType().Equals(typeof(SqlException)))
                return ParseSQLException((SqlException)exception);
            else
                return ParseBaseException(exception,useNewLine);  

        }

        public static int[] ProcessIDString(string ids)
        {

            string[] st = ids.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
            if (st.Length == 0) return null;

            int[] id = new int[st.Length];
            for (int l = 0; l < st.Length; l++)
            {
                if (!int.TryParse(st[l], out id[l]))
                    return null;
            }

            return id;
        }

        public static string CheckAndClearIDString(string ids)
        {

            string[] st = ids.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
            if (st.Length == 0) return "";

            int[] id = new int[st.Length];
            for (int l = 0; l < st.Length; l++)
            {
                if (!int.TryParse(st[l], out id[l]))
                    return "";
            }

            return string.Join(",",st) ;
        }

        #region Instance methods


        public Exception ProcessException;
        public bool ProcessStarted;

        public Task<object> RunProcessAsync(Process process, CancellationToken ctoken)
        {
            var tcs = new TaskCompletionSource<object>();

            bool exited = false;
            process.EnableRaisingEvents = true;

            CancellationTokenRegistration ctok_reg = ctoken.Register(() =>
            {
                process.Kill();
            });

            process.Exited += (sender, args) =>
            {

                if (exited)
                    return;

                try
                {

                    exited = true;

                    var errorMessage = process.StandardError.ReadToEnd();

                    if (!ctoken.IsCancellationRequested && (process.ExitCode != 0 || !string.IsNullOrWhiteSpace(errorMessage)))
                    {
                        tcs.SetException(new InvalidOperationException(
                            string.Format("The process did not exit correctly ({0}). The corresponding error message was: {1}", process.ExitCode, errorMessage))
                            );
                    }
                    else if (ctoken.IsCancellationRequested)
                    {
                        tcs.SetCanceled();
                    }
                    else
                    {
                        tcs.SetResult(null);
                    }
                }
                finally
                {
                    ctok_reg.Dispose();
                }

            };

            process.Start();

            return tcs.Task;
        }


        public async Task<object> RunProcessAsync(string filename, string arguments, CancellationToken ctoken)
        {
            ProcessException = null;
            object result = null;
            ProcessStarted = true;

            try
            {
                using (Process p = new Process())
                {
                    p.StartInfo.FileName = filename;
                    p.StartInfo.Arguments = arguments;
                    p.StartInfo.UseShellExecute = false;
                    p.StartInfo.RedirectStandardError = true;
                    p.StartInfo.CreateNoWindow = true;
                    p.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
                    result = await RunProcessAsync(p, ctoken);
                }
            }
            catch (TaskCanceledException)
            {

            }
            catch (Exception e)
            {
                ProcessException = e;
            }
            finally
            {
                ProcessStarted = false;
            }

            return result;
        }

        #endregion


    }
}
