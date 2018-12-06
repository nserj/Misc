using Helpers.Data;
using ICSharpCode.SharpZipLib.Core;
using ICSharpCode.SharpZipLib.GZip;
using ICSharpCode.SharpZipLib.Tar;
using ICSharpCode.SharpZipLib.Zip;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;

namespace DP2SaaSMCS.Tasks
{


    public class AWSUtils 
    {


        public static bool AnalysisResultExists(FQO fq, string tablename)
        {
            return (fq.ExecuteScalar<int>("select case when object_id(@tb,'U') is null then 0 else 1 end", CommandType.Text, "@tb", tablename) == 1);
        }

        public static enmFileFormats DetermineFormatOfFile(string filename)
        {

            if (!string.IsNullOrWhiteSpace(filename))
            {
                string ext = Path.GetExtension(filename);

                switch (ext.ToLowerInvariant())
                {
                    case ".xlsx":
                        return enmFileFormats.Excel;
                    case ".zip":
                        return (enmFileFormats.CSV | enmFileFormats.ZIP);
                    case ".gz":
                        return (enmFileFormats.CSV | enmFileFormats.GZ);
                    case ".csv":
                        return enmFileFormats.CSV;
                }
            }

            return enmFileFormats.Undefined;
        }


        public static object CreateInstance(string type)
        {
            return System.Reflection.Assembly.GetExecutingAssembly().CreateInstance(
            typeName: type,
            ignoreCase: false,
            bindingAttr: System.Reflection.BindingFlags.Default,
            binder: null,  // use default binder
            args: new object[] { },
            culture: null, // use CultureInfo from current thread
            activationAttributes: null
            );
        }

        public static awsFilePath ParseURLFilePath(string path, bool havefilename)
        {
            if (string.IsNullOrWhiteSpace(path))
                return null;

            if (!havefilename)
                path = path + "/x-name";

            awsFilePath fp = new awsFilePath();

            Uri u = new Uri(path);
            string[] sgs = u.Segments;
            fp.firstbucket = Uri.UnescapeDataString(sgs[1].Trim(new char[] { '/' }));
            if (sgs.Length > 3)
                fp.lastbucket = Uri.UnescapeDataString(sgs[sgs.Length - 2].Trim(new char[] { '/' }));
            else
                fp.lastbucket = fp.firstbucket;
            fp.keynameOriginal = string.Join("", sgs, 2, sgs.Length - 2);
            fp.keyname = Uri.UnescapeDataString(fp.keynameOriginal);
            fp.filename = Uri.UnescapeDataString(sgs[sgs.Length - 1]);
            fp.filefolder = Uri.UnescapeDataString(string.Join("", sgs, 2, sgs.Length - 3).Replace('/', '\\').Trim(new char[] { '\\' }));
            fp.full_bucket_path = Uri.UnescapeDataString(string.Join("", u.Segments, 1, u.Segments.Length - 2).TrimEnd('/'));

            return fp;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="filename"></param>
        /// <param name="trgPathOrFile">If format is ZIP - Path to folder of output files, GZ - Full file path</param>
        /// <param name="format">Zip,GZ</param>
        /// <returns></returns>
        public static int UnPackZipFile(string filename, string trgPathOrFile, enmFileFormats format)
        {

            if ((format & enmFileFormats.ZIP) > 0)
            {
                if (!Directory.Exists(trgPathOrFile))
                    Directory.CreateDirectory(trgPathOrFile);

                FastZip fs = new FastZip();
                fs.ExtractZip(filename, trgPathOrFile, FastZip.Overwrite.Always, null, "", "", true);
            }
            else if ((format & enmFileFormats.GZ) > 0)
            {

                string dir = Path.GetDirectoryName(trgPathOrFile);
                if (!Directory.Exists(dir))
                    Directory.CreateDirectory(dir);
                else if (File.Exists(trgPathOrFile))
                    File.Delete(trgPathOrFile);

                using (FileStream fs = File.OpenRead(filename))
                {
                    using (Stream inStream = new GZipInputStream(fs))
                    {
                        using (FileStream outStream = File.Create(trgPathOrFile))
                        {
                            byte[] buffer = new byte[4096];
                            StreamUtils.Copy(inStream, outStream, buffer);
                        }
                    }
                }
            }
            else
            {
                return -1;
            }

            return 1;
        }

        public static int PackZipFile(string destfilename, List<string> srcFiles, enmFileFormats format)
        {
            string dir = Path.GetDirectoryName(destfilename);

            if (File.Exists(destfilename))
                File.Delete(destfilename);
            else if (!Directory.Exists(dir))
                Directory.CreateDirectory(dir);

            if ((format & enmFileFormats.ZIP) > 0)
            {
                using (ZipFile zip = ZipFile.Create(destfilename))
                {
                    zip.BeginUpdate();
                    foreach (string f in srcFiles)
                    {
                        zip.Add(f, Path.GetFileName(f));
                    }
                    zip.CommitUpdate();
                    zip.Close();
                }
            }
            else if ((format & enmFileFormats.GZ) > 0)
            {
                TarCreateFromStream(destfilename, srcFiles);
            }
            else
            {
                return -1;
            }

            return 1;

        }

        public static void TarCreateFromStream(string destfilename, List<string> srcFiles)
        {
            using (Stream outStream = File.Create(destfilename))
            {
                // If you wish to create a .Tar.GZ (.tgz):
                // - set the filename above to a ".tar.gz",
                // - create a GZipOutputStream here
                // - change "new TarOutputStream(outStream)" to "new TarOutputStream(gzoStream)"
                // Stream gzoStream = new GZipOutputStream(outStream);
                // gzoStream.SetLevel(3); // 1 - 9, 1 is best speed, 9 is best compression

                using (GZipOutputStream gzoStream = new GZipOutputStream(outStream))
                {
                    gzoStream.SetLevel(4); // 1 - 9, 1 is best speed, 9 is best compression

                    using (TarOutputStream tarOutputStream = new TarOutputStream(gzoStream))
                    {
                        CreateTarManually(tarOutputStream, srcFiles);
                    }
                }
            }
        }

        protected static void CreateTarManually(TarOutputStream tarOutputStream, List<string> srcFiles)
        {

            // Optionally, write an entry for the directory itself.
            //
            /*    TarEntry tarEntry = TarEntry.CreateEntryFromFile(sourceDirectory);
                tarOutputStream.PutNextEntry(tarEntry);*/

            // Write each file to the tar.
            //
            TarEntry entry;
            string tarName;
            long fileSize;
            byte[] localBuffer = new byte[32 * 1024];
            int numRead;

            foreach (string filename in srcFiles)
            {
                using (Stream inputStream = File.OpenRead(filename))
                {
                    tarName = Path.GetFileName(filename);
                    fileSize = inputStream.Length;

                    // Create a tar entry named as appropriate. You can set the name to anything,
                    // but avoid names starting with drive or UNC.

                    entry = TarEntry.CreateTarEntry(tarName);

                    // Must set size, otherwise TarOutputStream will fail when output exceeds.
                    entry.Size = fileSize;

                    // Add the entry to the tar stream, before writing the data.
                    tarOutputStream.PutNextEntry(entry);

                    // this is copied from TarArchive.WriteEntryCore
                    while (true)
                    {
                        numRead = inputStream.Read(localBuffer, 0, localBuffer.Length);
                        if (numRead <= 0)
                        {
                            break;
                        }
                        tarOutputStream.Write(localBuffer, 0, numRead);
                    }
                }
                tarOutputStream.CloseEntry();
            }
        }


        private static string ChangeFileExtension(string fpath, enmFileFormats rformat)
        {
            string ret = fpath;

            if ((rformat & enmFileFormats.CSV) > 0)
            {

                string destFolder = Path.GetDirectoryName(fpath);
                string fname = Path.GetFileName(fpath);

                if (fname.EndsWith(".csv.gz", StringComparison.OrdinalIgnoreCase))
                    fname = fname.Substring(0, fname.Length - 7) + ".csv";
                else if (fname.EndsWith(".csv.zip", StringComparison.OrdinalIgnoreCase) ||
                         fname.EndsWith(".csv.csv", StringComparison.OrdinalIgnoreCase))
                    fname = fname.Substring(0, fname.Length - 8) + ".csv";
                else
                    fname = Path.ChangeExtension(fname, ".csv");

                ret = Path.Combine(destFolder, fname);
            }

            return ret;
        }


  
    }


}
