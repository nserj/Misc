using System;
using System.Runtime.Serialization;

namespace BaseWindowsService
{
    [Serializable]
    public class ExtendedException : Exception
    {
        public Exception BaseException { get; set; }
        public string Details { get; set; }

        public ExtendedException() { }
        public ExtendedException(Exception baseex, string message, string details, Exception innerexception) : base(message, innerexception)
        {
            BaseException = baseex;
            Details = details;
        }

        public ExtendedException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }

    }
}
