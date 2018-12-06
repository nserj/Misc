using Helpers.Data;
using System;
using System.Data;

namespace DP2SaaSMCS
{
    /// <summary>
    /// Database names to class
    /// </summary>
    [Serializable]
    public class DataBaseNames
    {
        public string Kernel;
        public string DataProfiling;
        public string Misc;
        public string DataProfilingSources;
        public string DataProfilingResults;
        public string Processing;

        public DataBaseNames() { }

        private static string CheckDbName(string db)
        {
            db = db.Trim(new char[] { '.', (char)32 });

            if (!string.IsNullOrWhiteSpace(db))
                return string.Concat(db, ".");

            return null;
        }

        public static DataBaseNames LoadNames(FQO fq)
        {
            DataBaseNames obj = new DataBaseNames();

            DataTable tb = fq.GetTable("select * from admin_DBNames", CommandType.Text, null);

            string bf;

            foreach (DataRow dr in tb.Rows)
            {
                bf = CheckDbName(dr["name"].ToString());

                switch (dr["UID"].ToString())
                {
                    case "DATA_PROFILING":
                        obj.DataProfiling = bf;
                        break;
                    case "KERNEL":
                        obj.Kernel = bf;
                        break;
                    case "MISCELLANEOUS":
                        obj.Misc = bf;
                        break;
                    case "DATA_SOURCES":
                        obj.DataProfilingSources = bf;
                        break;
                    case "DATA_RESULTS":
                        obj.DataProfilingResults = bf;
                        break;
                    case "PROCESSING":
                        obj.Processing = bf;
                        break;
                }
            }


            return obj;
        }
    }
}
