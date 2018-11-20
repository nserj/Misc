using System;
using System.Collections.Generic;
using System.Configuration;
using System.Collections;
using System.Linq;
using NSHelpers;

namespace BaseWindowsService
{

    public class WSvcException
    {

        public const int HANDLED_ERROR = 90000;
        public const int HANDLED_UKNOWN = HANDLED_ERROR-1;

        protected static Dictionary<int, string> ExceptionPairs;

        public int Number { get; set; }
        public string Message { get; set; }
        public Exception ExceptionObj { get; set; }
        public DateTime TimeStamp { get; set; } = DateTime.Now; 

        static WSvcException()
        {
            LoadDescriptions();
        }


        public static void LoadDescriptions()
        {
            Hashtable section = (Hashtable)ConfigurationManager.GetSection("errorsdescription");
            ExceptionPairs = section.Cast<DictionaryEntry>().ToDictionary(d => Convert.ToInt32(d.Key), d => (string)d.Value);

            ExceptionPairs[0]= "";
            ExceptionPairs[HANDLED_ERROR] = "Handled error";
            ExceptionPairs[HANDLED_UKNOWN] = "Unknown Error occurred";
        }

        public WSvcException() { }

        public bool IsEmpty
        {
            get { return Number == 0; }
        }

        public static WSvcException GetEmpty()
        {
            WSvcException ex = new WSvcException();
            ex.Clear();

            return ex;
        }

        public static WSvcException Create(int num, Exception exp)
        {
            WSvcException ex = new WSvcException();
            ex.Set(num, exp);

            return ex;
        }

        public void Clear()
        {
            Number = 0;
            Message = "";
            ExceptionObj = null;
            TimeStamp = DateTime.Now;
        }

        public WSvcException Copy()
        {
            WSvcException cp = new WSvcException();
            cp.Number = Number;
            cp.Message = String.Copy(Message);
            cp.TimeStamp = TimeStamp;
            if (ExceptionObj != null)
                cp.ExceptionObj = ExceptionObj.Clone<Exception>();

            return cp;
        }

        public WSvcException Set(int num)
        {
            return Set(num, null);
        }

        public WSvcException Set(int num,  Exception ex)
        {
            return Set(num, "", ex);
        }

        public WSvcException Set( Exception ex)
        {
            return Set(HANDLED_ERROR, "", ex);
        }

        public WSvcException Set(int num, string msg, Exception ex)
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

                string ems = Helpers.ExceptionToStringForMessage(ExceptionObj);

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
