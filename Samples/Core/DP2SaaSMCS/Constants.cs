using System;

namespace DP2SaaSMCS
{

    [Flags]
    public enum enmFileFormats
    {
        Undefined = 0,
        CSV = 2,
        Excel = 4,
        ZIP = 8,
        GZ = 16
    }

    [Flags]
    public enum enmLogSeverity
    {
        None = 0,
        Default = 2,
        Info = 4,
        Debug = 8,
        Details = 16,
        Warning = 32
    }

}
