using System;
using System.Drawing;
using System.Reflection;

namespace FWServiceAdmin.Code
{
    public class ReflectionTools
    {

        /// <summary>
        /// Convert object to PropertyValue class. ExtendedAttribute describes friendly name, color and default value.
        /// List object will be ignored
        /// </summary>
        /// <param name="atype"></param>
        /// <returns></returns>
        public static PropertyValueCollection ParseType(object atype)
        {
            if (atype == null) return new PropertyValueCollection();
            Type t = atype.GetType();
            ExtendedAttribute ea;
            PropertyValue pv;
            Type defType;
            DateTime defDate;

            PropertyInfo[] props = t.GetProperties();

            PropertyValueCollection dict = new PropertyValueCollection();

            foreach (PropertyInfo prp in props)
            {
                if (string.CompareOrdinal(prp.PropertyType.Name, "List`1") == 0 || prp.PropertyType.IsGenericType)
                    continue;

                var value = prp.GetValue(atype, new object[] { });

                ea = prp.GetCustomAttribute<ExtendedAttribute>();
                pv = new PropertyValue(value, Color.Black)
                {
                    Name = prp.Name,
                    FriendlyName = prp.Name
                };

                if (ea != null)
                {
                    if (ea.Default != null && !string.IsNullOrWhiteSpace(ea.Default.ToString()))
                    {
                        var eaval = ea.Default;
                        defType = Type.GetType(ea.DefaultType);

                        if (defType.Equals(typeof(DateTime)))
                        {
                            if (DateTime.TryParse(eaval.ToString(), out defDate))
                                eaval = defDate;
                            else
                                eaval = null;
                        }

                        if (eaval == null)
                        {
                            pv.ValueColor = Color.Brown;
                        }
                        else
                        {
                            pv.ValueColor = (eaval.Equals(value) ? Color.Blue : (ea.Importance > 50 ? Color.Red : Color.Green));
                        }
                    }

                    if (!string.IsNullOrWhiteSpace(ea.FriendlyName))
                        pv.FriendlyName = ea.FriendlyName;

                }

                dict.Add( pv);
            }
            return dict;
        }

    }
}
