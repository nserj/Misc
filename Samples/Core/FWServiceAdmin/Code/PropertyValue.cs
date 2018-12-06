using System;
using System.Collections.Generic;
using System.Drawing;

namespace FWServiceAdmin.Code
{
    public class PropertyValueCollection:List<PropertyValue>
    {

    }

    /// <summary>
    /// Class will be used to automatically fill UI table
    /// </summary>
    public class PropertyValue:IEquatable<PropertyValue>,IComparable<PropertyValue>
    {
        public string Name;
        public string FriendlyName;
        public object Value;
        public Color ValueColor;

        public PropertyValue() { }
        public PropertyValue(object val, Color cl) { Value = val; ValueColor = cl; }

        public int CompareTo(PropertyValue other)
        {
            if (other == null)
                return -1;

            return string.Compare(Name, other.Name);
        }

        public bool Equals(PropertyValue other)
        {
            if (other == null)
                return false;

            return (string.Compare(Name, other.Name) == 0);
        }
    }
}
