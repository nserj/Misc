using Microsoft.AspNetCore.Routing;
using System;

namespace FWServiceAdmin.Code
{
    public static  class Extension
    {

        public static string GetRequiredString(this RouteData routeData, string keyName)
        {
            object value;
            if (!routeData.Values.TryGetValue(keyName, out value))
            {
                throw new InvalidOperationException($"Could not find key with name '{keyName}'");
            }

            return value?.ToString();
        }

    }
}
