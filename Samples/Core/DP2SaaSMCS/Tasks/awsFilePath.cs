using System;

namespace DP2SaaSMCS.Tasks
{
    [Serializable]
    public class awsFilePath
    {
        public string firstbucket;
        public string lastbucket;
        public string keyname;
        public string keynameOriginal;
        public string filename;
        public string filefolder;
        public string full_bucket_path;

        public awsFilePath() { }
    }

}
