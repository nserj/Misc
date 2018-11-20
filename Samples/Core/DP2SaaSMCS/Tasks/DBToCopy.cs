using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DP2SaaSMCS.Tasks
{

    public class DBCopyHeader
    {
        public string Bucket;
        public string Destination;
        public string Source;

        public DBCopyHeader(string bucket, string destnation, string source)
        {
            Bucket = bucket;
            Destination = destnation;
            Source = source;
        }

    }

    public class DBToCopy
    {
        public string[] Source;
        public string Destination;
        public string DestinationBucket;
        public List<string> ChangedItemsName = new List<string>();

        public DBToCopy() { }
        public DBToCopy(string destbucket, string dest, string[] src)
        {
            DestinationBucket = destbucket;
            Destination = dest;
            Source = src;
        }

        public string ConcatenateChangedItemsName()
        {
            StringBuilder sb = new StringBuilder();
            var grp = ChangedItemsName.GroupBy(u => u).Select(u => u.Key);

            foreach (string s in grp)
                sb.Append(s).Append(", ");

            if (sb.Length == 0)
                return "";

            return sb.ToString(0, sb.Length - 2);

        }
    }


    public class DBToCopyList : List<DBToCopy>
    {
        public DBToCopyList() { }

        public DBToCopyList(DBCopyHeader[] ar)
        {
            var dest = ar.GroupBy(u => new { u.Bucket, u.Destination }).Select(u => u.Key).ToArray();

            foreach (var s in dest)
            {
                Add(new DBToCopy(s.Bucket, s.Destination, ar.Where(u => u.Bucket == s.Bucket && u.Destination == s.Destination).GroupBy(g => g.Source).Select(e => e.Key).ToArray()));
            }
        }
    }
}
