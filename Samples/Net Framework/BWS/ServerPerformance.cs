using NSHelpers.Storage.CSV;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms.DataVisualization.Charting;
using System.Drawing;

namespace BaseWindowsService.Performance
{

    public class WIKIPerformanceCountersHistoryItem
    {
        public DateTime TimeStamp { get; set; }
        public string MeasurePeriod;
        public Guid CounterUID { get; set; }
        public float Value { get; set; }
        public float MinValue { get; set; }
        public float MaxValue { get; set; }
        public float AvgValue { get; set; }
        public int DividerForAverage { get; set; }
        public int MinValueFreq { get; set; }
        public int MaxValueFreq { get; set; }

        public WIKIPerformanceCountersHistoryItem() { }
    }


    public class WIKIPerformanceCountersHistory
    {
        public int MeasurementsCount;
        public List<WIKIPerformanceCountersHistoryItem> Items = new List<WIKIPerformanceCountersHistoryItem>();
        public WIKIPerformanceCountersHistory() { }
    }

    public class WIKIPerformanceCounter
    {
        public Guid UID = Guid.NewGuid();
        public PerformanceCounter Counter;
        public string CounterFiendlyName { get; set; }
        public string InstanceFiendlyName { get; set; }
        public bool SummaryExcludeZeroValues { get; set; }
        public int ValueDivider { get; set; }
        public string Description { get; set; }
        public string ChartGroup { get; set; }

        public WIKIPerformanceCounter() { }

        public WIKIPerformanceCounter(string category, string countername, string instancename)
        {
            if (string.IsNullOrWhiteSpace(instancename))
                Counter = new PerformanceCounter(category, countername, true);
            else
                Counter = new PerformanceCounter(category, countername, instancename, true);
        }

    }

    public class ObjectsInPerformance
    {
        private DateTime _timeStamp = DateTime.Now;

        public string Name { get; set; }
        public DateTime TimeStamp { get => _timeStamp; set => _timeStamp = value; } 

        public ObjectsInPerformance() { }

        public override string ToString()
        {
            return string.Format("{0} - {1:G}",Name,TimeStamp);
        }
    }


    public class ServerPerformance : IDisposable
    {

        public bool CancelAsync;
        public bool InProcess;
        public Task CurrentTaks;
        public bool Paused;
        public List<ObjectsInPerformance> DetailObjects = new List<ObjectsInPerformance>();
        public WIKIPerformanceCountersHistory History = new WIKIPerformanceCountersHistory();

        int performanceCounterMeasureIntervalMSec;

        Dictionary<Guid, WIKIPerformanceCounter> counters = new Dictionary<Guid, WIKIPerformanceCounter>();

        public ServerPerformance(string cfgdata, int measureInterval)
        {

            performanceCounterMeasureIntervalMSec = measureInterval;

            string[] block = cfgdata.Replace("\n", "").Replace("\r", "").Replace("\t", "").Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries);

            string[] prm;
            WIKIPerformanceCounter c;
            bool bf;
            int bfint;

            foreach (string s in block)
            {
                prm = s.Split(new char[] { ',' }, StringSplitOptions.None);

                if (!bool.TryParse(prm[5].Trim(), out bf))
                {
                    throw new ArgumentException("Incorrect value of SummaryExcludeZeroValues parameter (bool)");
                }

                if (!int.TryParse(prm[6].Trim(), out bfint))
                {
                    throw new ArgumentException("Incorrect value of ValueDivider parameter (int)");
                }

                c = new WIKIPerformanceCounter(category: prm[0].Trim(), countername: prm[1].Trim(), instancename: prm[2].Trim())
                {
                    CounterFiendlyName = prm[3].Trim(),
                    InstanceFiendlyName = prm[4].Trim(),
                    SummaryExcludeZeroValues = bf,
                    ValueDivider = bfint,
                    ChartGroup= prm[7].Trim(),
                    Description = prm[8].Trim()
                };

                counters.Add(c.UID, c);
            }

        }


        public List<WIKIPerformanceCountersHistoryItem> GetAverageValues()
        {

            List<WIKIPerformanceCountersHistoryItem> ret = new List<WIKIPerformanceCountersHistoryItem>();

            if (History.Items.Count > 0)
            {

                WIKIPerformanceCountersHistoryItem itm;

                DateTime MinDate = History.Items.Min(u => u.TimeStamp);
                DateTime MaxDate = History.Items.Max(u => u.TimeStamp);

                itm = new WIKIPerformanceCountersHistoryItem
                {
                    CounterUID = Guid.Empty,
                    MeasurePeriod = string.Format("{0} - {1}", MinDate.ToString("G"), MaxDate.ToString("G"))

                };

                ret.Add(itm);

                History.MeasurementsCount = -1;

                IEnumerable<WIKIPerformanceCountersHistoryItem> items;
                IEnumerable<WIKIPerformanceCountersHistoryItem> items_nozero;

                foreach (KeyValuePair<Guid, WIKIPerformanceCounter> c in counters)
                {
                    itm = new WIKIPerformanceCountersHistoryItem
                    {
                        CounterUID = c.Key,
                        DividerForAverage = 0
                    };

                    items = History.Items.Where(z => z.CounterUID == c.Key);

                    if (History.MeasurementsCount < 0)
                    {
                        //count of each counter in the history is the same, so calculate iterations # by the first in the history
                        History.MeasurementsCount = items.Count();
                    }

                    if (c.Value.SummaryExcludeZeroValues)
                        items_nozero = items.Where(z => z.Value != 0);
                    else
                        items_nozero = items;

                    if (items_nozero.Count() > 0)
                    {
                        itm.MinValue = items_nozero.Min(k => k.Value);
                        itm.MaxValue = items_nozero.Max(k => k.Value);
                    }
                    else
                    {
                        itm.MinValue = -1;
                        itm.MaxValue = -1;
                    }

                    itm.MinValueFreq = items.Where(z => z.Value == itm.MinValue).Count();
                    itm.MaxValueFreq = items.Where(z => z.Value == itm.MaxValue).Count();

                    if (c.Value.Counter.CounterName.Substring(0, 5).ToLowerInvariant() != "avg. ")
                    {
                        itm.AvgValue = -1;
                        itm.DividerForAverage = items_nozero.Count();

                        if (itm.DividerForAverage > 0)
                            itm.AvgValue = items.Sum(u => u.Value) / itm.DividerForAverage;
                    }
                    else
                        itm.AvgValue = itm.MaxValue;

                    ret.Add(itm);
                }
            }

            return ret;
        }

        public void GenerateDetailCharts(string dir)
        {
           var groups = counters.Where(l=>!string.IsNullOrWhiteSpace(l.Value.ChartGroup)).GroupBy(u => u.Value.ChartGroup).Select(g => g.Key);

            using (Chart chart1 = new Chart())
            {

                if (chart1.ChartAreas.Count == 0)
                    chart1.ChartAreas.Add("chrt_area");

                if (chart1.Legends.Count == 0)
                    chart1.Legends.Add(new Legend("legend1"));

                foreach (string grp in groups)
                {

                    IEnumerable<WIKIPerformanceCounter> flt_grp = counters.Where(l => l.Value.ChartGroup == grp).Select(z => z.Value);

                    chart1.Series.Clear();
                    chart1.Size = new Size(2000, 800);

                    int idx = 0;
                    double min = double.MaxValue;
                    double max = double.MinValue;
                    double bf;

                    foreach (WIKIPerformanceCounter currRow in flt_grp)
                    {
                        var drSer = History.Items.Where(u => u.CounterUID == currRow.UID);

                        if (drSer.Count() > 0)
                        {
                            bf = drSer.Select(u => u.Value).Max();
                            if (max < bf)
                                max = bf;

                            bf = drSer.Select(u => u.Value).Min();
                            if (min > bf)
                                min = bf;

                            Series s = new Series("ser" + grp + idx++.ToString());
                            s.ChartType = SeriesChartType.Line;
                            s.Points.DataBind(drSer, "TimeStamp", "Value", "");
                            s.LegendText = string.Format("{0} ({1})", currRow.CounterFiendlyName, currRow.InstanceFiendlyName);
                            s.Legend = "legend1";
                            s.BorderWidth = 2;
                            chart1.Series.Add(s);
                        }
                    }

                    if (chart1.Series.Count > 0)
                    {
                        if (max < 0) max = 0;
                        if (min < 0) min = 0;

                        if (max != min)
                        {
                            chart1.ChartAreas[0].AxisY.Maximum = max;
                            chart1.ChartAreas[0].AxisY.Minimum = min;
                        }
                        if( max == 0 &&  min == 0)
                            chart1.ChartAreas[0].AxisY.Minimum = min;

                        chart1.ChartAreas[0].AxisX.LabelStyle.Format = "G";
                        chart1.DataBind();

                        string fname = Path.Combine(dir, string.Format("PrfGroup_{0}.jpeg", grp));

                        chart1.SaveImage(imageFileName: fname, format: ChartImageFormat.Jpeg);
                    }
                }
            }
        }


        public void WriteToFile(string file, bool writedetails, bool generateCharts)
        {
            string filename = Path.GetFileName(file);
            string filenameExt = Path.GetExtension(filename);

            DateTime currTime = DateTime.Now;

            string dir = Path.GetDirectoryName(file);
            dir = Path.Combine(dir, "Prf_" + currTime.ToString("MM_dd_yyyy_HH_mm_ss"));
            if (!Directory.Exists(dir))
            {
                Directory.CreateDirectory(dir);
            }

            if(generateCharts)
            {
                GenerateDetailCharts(dir);
            }


            dir = Path.Combine(dir, filename);

            if (string.IsNullOrWhiteSpace(filenameExt))
            {
                filenameExt = ".csv";
                dir = string.Concat(dir,filenameExt);
            }
            else
            {
                if (string.Compare(filenameExt, ".csv", StringComparison.OrdinalIgnoreCase) != 0)
                {
                    dir = dir.Replace(filenameExt, ".csv");
                    filenameExt = ".csv";
                }
            }

            WIKIPerformanceCounter c;

            if (writedetails)
            {
                using (CsvFileWriter w = new CsvFileWriter(dir, Encoding.UTF8))
                {
                    w.WriteRow(new string[] { "Date", "Instance", "Category", "CounterName", "Value", "Description" });

                    foreach (WIKIPerformanceCountersHistoryItem i in History.Items)
                    {
                        c = counters[i.CounterUID];
                        w.WriteRow(new string[] { i.TimeStamp.ToString("G"), c.InstanceFiendlyName, c.Counter.CategoryName, c.CounterFiendlyName, i.Value.ToString(), c.Description });
                    }
                }
            }

            dir = dir.Replace(filenameExt, "_AVG" + filenameExt);

            List<WIKIPerformanceCountersHistoryItem> lst = GetAverageValues();

            using (CsvFileWriter w = new CsvFileWriter(dir, Encoding.UTF8))
            {
                w.WriteRow(new string[] { "Instance", "Category", "CounterName", "MinValue", "MaxValue", "AVGValue","Divider for Average", "MinValue Freq", "MaxValue Freq" ,"Chart Group","Description" });

                foreach(ObjectsInPerformance o in DetailObjects)
                {
                    w.WriteRow(new string[] { "", "", "", "", "", "", "", "", o.ToString()});
                }

                foreach (WIKIPerformanceCountersHistoryItem ci in lst)
                {
                    if (ci.CounterUID == Guid.Empty)
                        w.WriteRow(new string[] { "", "", "", "", "", "", "", "", string.Format("{0}, measurements #: {1}", ci.MeasurePeriod, History.MeasurementsCount) });
                    else
                    {
                        c = counters[ci.CounterUID];
                        w.WriteRow(new string[] { c.InstanceFiendlyName, c.Counter.CategoryName, c.CounterFiendlyName, ci.MinValue.ToString(), ci.MaxValue.ToString(),
                                                  ci.AvgValue.ToString(),ci.DividerForAverage.ToString(),
                                                  ci.MinValueFreq.ToString(),ci.MaxValueFreq.ToString(),
                                                  c.ChartGroup,
                                                  c.Description });
                    }
                }
            }
        }

        public Task CollectAsync()
        {
            CancelAsync = false;

            if (CurrentTaks != null) CurrentTaks.Dispose();
            CurrentTaks = Task.Run(() => CollectContinuosly());
            return CurrentTaks;
        }

        public void CollectContinuosly()
        {

            InProcess = true;

            History = new WIKIPerformanceCountersHistory();
            int curr_interval;
            bool firstStep = true;


            try
            {
                while (!CancelAsync)
                {
                    if (!Paused)
                    {
                        Collect();
                        if (firstStep)
                        {
                            History.Items.Clear();
                            firstStep = false;
                        }
                    }

                    curr_interval = 0;
                    while (curr_interval < performanceCounterMeasureIntervalMSec && !CancelAsync)
                    {
                        Thread.Sleep(100);
                        curr_interval += 100;
                    }
                }
            }
            finally
            {
                InProcess = false;
            }

        }


        public void Collect()
        {
            float val;
            WIKIPerformanceCountersHistoryItem itm;
            foreach (KeyValuePair<Guid, WIKIPerformanceCounter> c in counters)
            {
                val = c.Value.Counter.NextValue();
                if (c.Value.ValueDivider > 0)
                    val = val / c.Value.ValueDivider;

                itm = new WIKIPerformanceCountersHistoryItem()
                {
                    TimeStamp = DateTime.Now,
                    CounterUID = c.Key,
                    Value = val
                };

                History.Items.Add(itm);
            }
        }


        #region IDisposable Support
        private bool disposedValue = false;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {

                    if (CurrentTaks != null) CurrentTaks.Dispose();
                    foreach (KeyValuePair<Guid, WIKIPerformanceCounter> c in counters)
                    {
                        if (c.Value != null) c.Value.Counter.Dispose();
                    }
                }
                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }
        #endregion


    }
}
