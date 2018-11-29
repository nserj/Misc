using System;

namespace FWServiceAdmin.Code
{
    [AttributeUsage(AttributeTargets.Property)]
    [Serializable]
    public class ExtendedAttribute : Attribute
    {
        public object Default { get; set; }
        public string DefaultType { get; set; }
        public string FriendlyName { get; set; } = "";
        public int Importance { get; set; } = 0;

        public ExtendedAttribute(string fr_name, object defval, Type deftype, int imp)
        {
            FriendlyName = fr_name;
            Default = defval;
            DefaultType = deftype.FullName;
            Importance = imp;
        }

        public ExtendedAttribute(string fr_name, object defval, int imp)
        {
            FriendlyName = fr_name;
            Default = defval;
            DefaultType = defval.GetType().FullName;
            Importance = imp;
        }
    }
}
