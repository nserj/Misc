using Helpers;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;


namespace DP2SaaSMCS
{

    public interface IMCSException
    {
        int Number { get; set; }
        string Message { get; set; }
        DateTime TimeStamp { get; set; }
        bool IsEmpty { get; }
        IMCSException Copy();
        IMCSException Set(int num);
        IMCSException Set(int num, Exception ex);
        IMCSException Set(Exception ex);
        IMCSException Set(int num, string msg, Exception ex);
        void Clear();
    }

    /// <summary>
    /// Exception class.
    /// </summary>
    [DataContract]
    public class MCSException: IMCSException
    {

        public const int HANDLED_ERROR = 90000;
        public const int HANDLED_UKNOWN = HANDLED_ERROR-1;

        [DataMember]
        public int Number { get; set; }
        [DataMember]
        public string Message { get; set; }
        [DataMember]
        public DateTime TimeStamp { get; set; } = DateTime.Now;

        public Exception ExceptionObj { get; set; }

        private IConfiguration configuration;
        protected static Dictionary<int, string> ExceptionPairs;


        public MCSException(IConfiguration _configuration)
        {
            configuration = _configuration;

            if (configuration != null && ExceptionPairs == null)
                LoadDescriptions();

            Clear();
        }

        /// <summary>
        /// Load predefined description pairs of exceptions from config file
        /// </summary>
        public void LoadDescriptions()
        {

            ExceptionPairs = configuration.GetSection("ErrorsDescription")
                                .GetChildren().ToDictionary(x => Convert.ToInt32(x.Key), x => x.Value);



            ExceptionPairs[0] = "";
            ExceptionPairs[HANDLED_ERROR] = "Handled error";
            ExceptionPairs[HANDLED_UKNOWN] = "Unknown Error occurred";
        }

        public bool IsEmpty
        {
            get { return Number == 0; }
        }

        public void Clear()
        {
            Number = 0;
            Message = "";
            ExceptionObj = null;
            TimeStamp = DateTime.Now;
        }

        public IMCSException Copy()
        {
            MCSException cp = new MCSException(null);
            cp.Number = Number;
            cp.Message = String.Copy(Message);
            cp.TimeStamp = TimeStamp;
            if (ExceptionObj != null)
                cp.ExceptionObj = ExceptionObj.Clone<Exception>();

            return cp;
        }

        public IMCSException Set(int num)
        {
            return Set(num, null);
        }

        public IMCSException Set(int num,  Exception ex)
        {
            return Set(num, "", ex);
        }

        public IMCSException Set( Exception ex)
        {
            return Set(HANDLED_ERROR, "", ex);
        }

        public IMCSException Set(int num, string msg, Exception ex)
        {
            Clear();

            if (!ExceptionPairs.ContainsKey(num))
            {
                Number = HANDLED_UKNOWN;
                Message = string.Format("[{0}:{1}]: {2} ", HANDLED_UKNOWN, num, ExceptionPairs[HANDLED_UKNOWN]);
            }
            else if (num > 0)
            {
                Number = num;
                Message = string.Format("[{0}]: {1}", num, ExceptionPairs[num]);
            }

            if(!string.IsNullOrWhiteSpace(msg))
            {
                Message = string.Concat(Message, " [", msg, "]");
            }

            ExceptionObj = ex;

            if (ExceptionObj != null)
            {
                string s = Message;

                string ems = Utils.ExceptionToStringForMessage(ExceptionObj);

                if (string.IsNullOrWhiteSpace(s))
                    s = ems;
                else
                    s = string.Concat(s, ". ", ems);

                Message = s;
            }

            return this;
        }

    }
}
