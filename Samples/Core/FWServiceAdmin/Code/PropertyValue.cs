using System.Drawing;

namespace FWServiceAdmin.Code
{
    public class PropertyValue
    {
        public string Name;
        public object Value;
        public Color ValueColor;

        public PropertyValue(object val, Color cl) { Value = val; ValueColor = cl; }
    }
}
