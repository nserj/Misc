using NSHelpers;
using NSHelpers.XML;
using System;
using System.Collections.Generic;
using System.Xml.Serialization;
using System.Linq;


namespace BaseWindowsService.Workers
{
    public interface IStepsConfig
    {
        string MD5 { get; set; }
        void CalcMD5();
        int Count { get; }
    }

    [Serializable, XmlRoot(ElementName = "StepsConfig")]
    public class StepsConfig<T> : IStepsConfig
    {

        public StepsConfig() { }

        [XmlArrayItem("Step"), XmlArray("Steps")]
        public List<T> Items { get; set; } = new List<T>();

        [XmlIgnore]
        public int Count { get { return Items.Count; }  } 

        [XmlIgnore]
        public string MD5 { get; set; } = "";

        public List<object> GetItems()
        {
            return Items.Cast<object>().ToList();
        }

        public void CalcMD5()
        {
            MD5 = CryptoCore.GetMD5Hash(XmlManager.SerializeToString(this, true));
        }


    }

    [Serializable, XmlRoot(ElementName = "StepConfig")]
    public class StepConfig
    {

        public StepConfig() { }
        public ExecuteConditions ExecuteCondition { get; set; } = new ExecuteConditions();

    }
}
