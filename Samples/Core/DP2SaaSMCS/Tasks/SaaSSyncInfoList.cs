using Helpers.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Xml.Serialization;

namespace DP2SaaSMCS.Tasks
{
    [Serializable]
    public class SaaSSyncInfo
    {
        [XmlAttribute]
        public int ID { get; set; }
        [XmlAttribute]
        public int UploadType { get; set; }
        [XmlAttribute]
        public string SourceTable { get; set; }
        [XmlAttribute]
        public string SourceDataBase { get; set; }
        [XmlAttribute]
        public string ResultDataBase { get; set; }
        [XmlAttribute]
        public string ResultBucket { get; set; } = "";

        [XmlAttribute]
        public int SyncType { get; set;} = 2; /*1 - upload, 2 - dictionary*/
        [XmlAttribute]
        public int SourceTypes { get; set; } = 1; /*1 - Table or View , 2 - Stored Procedure*/



        /*optional properties*/
        [XmlAttribute]
        public string UploadName { get; set; }

        [XmlIgnore]
        public bool WasChanged { get; set; }
        [XmlIgnore]
        public long Change_Tracking_Last_Version { get; set; } = 0;


        public SaaSSyncInfo() { }
        public SaaSSyncInfo(int utype, string utable, string udb, string resdb,string bucket, int syncType)
        {
            UploadType = utype;
            SourceTable = utable;
            SourceDataBase = udb;
            SyncType = syncType;
            ResultDataBase = resdb;
            ResultBucket = bucket;
        }

        public static SaaSSyncInfo GetObject(DataRow dr)
        {
            DataRowHelper dh = new DataRowHelper(dr);
            SaaSSyncInfo bf = new SaaSSyncInfo(dh.GetInt("UploadType_id"), dh.GetString("SourceTable"), dh.GetString("SourceDatabase"), dh.GetString("ResultDatabase"),
                                               dh.GetString("ResultBucket"), dh.GetInt("SyncType"))
            {
                UploadName = dh.GetString("UploadName"),
                WasChanged = dh.GetValue<bool>("waschanged"),
                Change_Tracking_Last_Version = dh.GetValue<long>("Change_Tracking_Last_Version"),
                SourceTypes= dh.GetInt("SourceType")
            };
            return bf;
        }


    }

    [Serializable]
    [XmlRoot("SaaSSyncInfoList")]
    public class SaaSSyncInfoList : List<SaaSSyncInfo>
    {
        public SaaSSyncInfoList() { }


        public void AddRange(DataTable dt)
        {
            foreach (DataRow dr in dt.Rows)
            {
                this.Add(SaaSSyncInfo.GetObject(dr));
            }
        }

    }


}
