using System.Configuration;
using System.IO;
using System.Text;
using System.Xml;
using System.Xml.Linq;
using System.Xml.Serialization;

namespace BaseWindowsService
{
    public class XmlConfigurationSection : ConfigurationSection
    {
        private XDocument document;

        protected override void DeserializeSection(XmlReader reader)
        {
            this.document = XDocument.Load(reader);
        }

        protected override object GetRuntimeObject()
        {
            return this.document;
        }
    }

    public class AppSection<T>
    {
        private readonly string sectionName;
        private readonly XmlSerializer serializer;

        public T Instance;

        public AppSection(string secname)
        {
            sectionName = secname;
            serializer = new XmlSerializer(typeof(T), new XmlRootAttribute(sectionName));
        }

        public T Get()
        {
            var document = (XDocument)ConfigurationManager.GetSection(sectionName);
            Instance = (T)serializer.Deserialize(document.CreateReader());
            return Instance;
        }

        public string Serialize(T obj)
        {
            StringBuilder sb = new StringBuilder();

            using (StringWriter t = new StringWriter(sb))
            {
                serializer.Serialize(t, obj);
            }

            return sb.ToString();
        }


    }
}
