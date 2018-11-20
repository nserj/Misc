using System;
using System.Collections.Generic;
using System.Data;
using Helpers;

namespace DP2SaaSMCS.Tasks
{
    public class SQLTableParameters
    {

        public static string IntLongArrayDBType
        {
            get { return "dbo.IntLongArray"; }
        }

        public static DataTable Get_IntLongArray()
        {
            DataTable tb = new DataTable("IntLongArray");

            tb.Columns.Add("ValueInt", typeof(int));
            tb.Columns.Add("ValueLong", typeof(long));
            tb.Constraints.Add("primiaad", new DataColumn[] { tb.Columns[0], tb.Columns[1] }, true);
            return tb;
        }

        public static DataTable Get_IntArray()
        {
            DataTable tb = new DataTable("IntArray");
            tb.Columns.Add("Value", typeof(Int32));
            tb.Constraints.Add("primia", new DataColumn[] { tb.Columns[0] }, true);
            return tb;
        }

        public static string IntArrayDBType
        {
            get { return "dbo.IntArray"; }
        }

        public static DataTable Get_IntArray(string ids)
        {
            int[] ar = null;
            if (!string.IsNullOrWhiteSpace(ids))
            {
                ar = ids.Split(',').ToIntArray();
            }
            return Get_IntArray(ar);
        }

        public static DataTable Get_IntArray(IEnumerable<int> ids)
        {
            DataTable tb = Get_IntArray();
            if (ids != null)
            {
                foreach (int i in ids)
                    tb.LoadDataRow(new object[] { i }, true);
            }
            return tb;
        }

    }
}
