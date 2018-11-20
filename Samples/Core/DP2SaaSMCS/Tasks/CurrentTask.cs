using Amazon.Extensions.NETCore.Setup;
using Amazon.S3;
using Amazon.S3.Model;
using Helpers.Data;
using Helpers.SqlLiteTool;
using Microsoft.Extensions.Configuration;
using ScheduledService.Code;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DP2SaaSMCS.Tasks
{

    public class CurrentTask : IScheduledTask
    {

        public string Schedule { get; set; }
        public bool CanBeExecute { get => !(State.InProcess || State.InInternalProcess || State.Freezed || State.EmergencyStopped); }
        public IConfiguration Configuration { get; }

        public TaskState State { get; set; }

       ScheduledTaskState IScheduledTask.State
        {
            get {return this.State; }
            set {this.State= (TaskState) value; }
        }

        private static NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();

        private DataBaseNames DbNames;
        private SaaSSyncInfoList infoObj;
        private DBToCopyList Dbs;

        private string liteTempPath;
        private string s3path;
        private string s3pathDefaultBucket;
        private string s3pathDefaultFull;
        private string dbConnectionString;
        private int dbSQLCommandTimeOut;

        private DBToCopy resultDB;

        private DataTable _processingData;
        private CancellationToken _cancellationToken;

        public CurrentTask(IConfiguration configuration, ScheduledTaskState state)
        {
            Configuration = configuration;
            Schedule = Configuration["RunConditions:Schedule"];
            dbConnectionString = Configuration["RunConditions:DBConnectionString"];
            dbSQLCommandTimeOut = Convert.ToInt32(Configuration["RunConditions:SQLCommandTimeOut"]);
            s3path = Configuration["RunConditions:S3BasePath"];

            if (!s3path.EndsWith("/"))
                s3path += "/";

            s3pathDefaultBucket = Configuration["RunConditions:S3BaseBucket"];
            s3pathDefaultFull = s3path + s3pathDefaultBucket;
            liteTempPath = Configuration["RunConditions:FileStoragePath"];

            State = (TaskState)state;

            using (FQO fq = GetFQO())
            {
                DbNames = DataBaseNames.LoadNames(fq);
            }
        }


        public async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            State.Clear();
            State.InProcess = true;
            DataTable tb = null;

            _cancellationToken = cancellationToken;
            _cancellationToken.Register(() => { State.ActionCancelled = true; });

            try
            {

                CheckForConditions();

                if (_processingData == null || _processingData.Rows.Count == 0)
                    return;

                CollectDataToCopy();


                if (infoObj.Count == 0)
                    return;


                /*get list of keys grouped by result, source databases and compose list of result DBs*/
                Dbs = new DBToCopyList(infoObj.GroupBy(u => new { u.ResultBucket, u.ResultDataBase, u.SourceDataBase }).
                                                         Select(a => new DBCopyHeader(a.Key.ResultBucket, a.Key.ResultDataBase, a.Key.SourceDataBase)).ToArray());


                if (Dbs.Count == 0)
                    return;


                State.InInternalProcess = true;

                for (int i = 0; i < Dbs.Count; i++)
                {

                    resultDB = Dbs[i];

                    if (_cancellationToken.IsCancellationRequested)
                        break;

                    if (string.IsNullOrWhiteSpace(resultDB.DestinationBucket))
                        resultDB.DestinationBucket = s3pathDefaultFull;
                    else
                        resultDB.DestinationBucket = s3path + resultDB.DestinationBucket;

                    await CopyDataBaseAsync();

                }

                if (_cancellationToken.IsCancellationRequested)
                    return;

                using (FQO fq = GetFQO())
                {

                    tb = SQLTableParameters.Get_IntArray(infoObj.Where(u => u.SyncType == 1).Select(a => a.UploadType).ToArray());
                    if (tb.Rows.Count > 0)
                    {
                        fq.ExecuteNonQuery(DbNames.Misc + "UploadsEvent_setSaaSSynchronized", CommandType.StoredProcedure,
                            new SqlParameter[] { FQO.BuildStructuredTypeParameter("@types", tb, SQLTableParameters.IntArrayDBType) });
                    }

                    tb = SQLTableParameters.Get_IntLongArray();

                    var lst = infoObj.Where(u => u.SyncType == 2);
                    foreach (SaaSSyncInfo i in lst)
                    {
                        tb.LoadDataRow(new object[] { i.UploadType, i.Change_Tracking_Last_Version }, true);
                    }

                    if (tb.Rows.Count > 0)
                    {
                        fq.ExecuteNonQuery(DbNames.Misc + "saas_SetDictionarySinchronized", CommandType.StoredProcedure,
                            new SqlParameter[] { FQO.BuildStructuredTypeParameter("@types", tb, SQLTableParameters.IntLongArrayDBType) });
                    }
                }
            }
            catch (Exception ex)
            {

                logger.Error(ex);
                ((TaskState)State).SetError(ex);
            }
            finally
            {
                State.InProcess = false;
                State.InInternalProcess = false;
            }

        }


        private void CheckForConditions()
        {
            using (FQO fq = GetFQO())
            {
                _processingData = fq.GetTable(DbNames.Misc + "saas_GetDataToSync");
            }
        }

        private async Task CopyToSQLite (int sourceID, string tmpFilePath)
        {
            SqlConnectionStringBuilder sb = new SqlConnectionStringBuilder(dbConnectionString);
            sb.InitialCatalog = resultDB.Source[sourceID];

            IEnumerable<SaaSSyncInfo> si;

            /*list of uploads (tables) by result and source dbs*/

            si = infoObj.Where(u => string.Compare(resultDB.Source[sourceID], u.SourceDataBase, StringComparison.OrdinalIgnoreCase) == 0 &&
                                               string.Compare(resultDB.Destination, u.ResultDataBase, StringComparison.OrdinalIgnoreCase) == 0
                                          );

            string[] currtables = si.Select(u => u.SourceTable).ToArray();
            resultDB.ChangedItemsName.AddRange(si.Where(ii => ii.WasChanged).Select(ii => ii.UploadName));

            using (SqlServerToSQLite sq = new SqlServerToSQLite())
            {
                /*create copy in result database*/
                sq.SQLCommandTimeout = dbSQLCommandTimeOut;
                await Task.Run(() => sq.CopySqlServerTablesToSQLiteFile(sb.ToString(), tmpFilePath, null, currtables, true, _cancellationToken)/*,_cancellationToken*/);
            }
        }

        private async Task CopyDataBaseAsync()
        {

            awsFilePath afp = AWSUtils.ParseURLFilePath(resultDB.DestinationBucket, false);


            string[] currtables = null;
            string tmpFilePath = "";

            /*result Db file*/
            afp.filename = resultDB.Destination.ToLowerInvariant();
            tmpFilePath = Path.Combine(liteTempPath, afp.filename);

            /*prepare copy of the database*/
            try
            {
                if (File.Exists(tmpFilePath))
                    File.Delete(tmpFilePath);

                for (int i = 0; i < resultDB.Source.Length; i++)
                {

                    if (_cancellationToken.IsCancellationRequested)
                        return;

                    await CopyToSQLite(i, tmpFilePath);

                }
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("Copy Data. Dbs:{0},  currtables: {1},tmpFilePath: {2} ",
                        Utils.SafeTostring(Dbs),  Utils.SafeTostring(currtables), Utils.SafeTostring(tmpFilePath)), ex);
            }


            if (_cancellationToken.IsCancellationRequested)
                return;

            /*send to S3 the prepared DB*/
            try
            {

                AWSOptions op = Configuration.GetAWSOptions();
                using (IAmazonS3 client = op.CreateServiceClient<IAmazonS3>())
                {

                    S3.AWSS3 aWSS3 = new S3.AWSS3(client, afp.full_bucket_path, afp.filename);

                    var vres = await aWSS3.FileMetadataAsync(_cancellationToken);

                    if (_cancellationToken.IsCancellationRequested)
                        return;

                    if (vres != null)
                    {
                        await aWSS3.DeleteFileAsync(_cancellationToken);

                        if (_cancellationToken.IsCancellationRequested)
                            return;
                    }


                    try
                    {
                        var brequest = new GetBucketLocationRequest()
                        {
                            BucketName = afp.full_bucket_path
                        };

                        await client.GetBucketLocationAsync(brequest, _cancellationToken);
                    }
                    catch
                    {
                        throw new Exception(string.Format("Send to S3. afp.full_bucket_path: {0}, afp.filename: {1}, tmpFilePath: {2}. Bucket does not exists."
                                                , Utils.SafeTostring(afp.full_bucket_path), Utils.SafeTostring(afp.filename), Utils.SafeTostring(tmpFilePath)));
                    }

                    PutObjectRequest request = new PutObjectRequest()
                    {
                        BucketName = afp.full_bucket_path,
                        Key = afp.filename,
                        FilePath = tmpFilePath
                    };

                    await client.PutObjectAsync(request, _cancellationToken);

                }

             //   RaiseOnWorkerInfo(string.Format("New data was sent for database [{0}]. Changed data [{1}]", resultDB.Destination, resultDB.ConcatenateChangedItemsName()), true, State);

            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("Send to S3. afp.full_bucket_path:{0}, afp.filename {1}, tmpFilePath: {2} "
                                        , Utils.SafeTostring(afp.full_bucket_path), Utils.SafeTostring(afp.filename), Utils.SafeTostring(tmpFilePath)), ex);
            }
        }



        private bool CollectDataToCopy()
        {
            try
            {

                if (!Directory.Exists(liteTempPath))
                    Directory.CreateDirectory(liteTempPath);

                infoObj = new SaaSSyncInfoList();
                infoObj.AddRange(_processingData);

            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("Load Config. infoObj:{0},  liteTempPath: {1} "
                                    , Utils.SafeTostring(infoObj), Utils.SafeTostring(liteTempPath)), ex);
            }

            return true;
        }

        public FQO GetFQO()
        {
            FQO fq = new FQO(dbConnectionString);
            fq.AutoCloseConnection = false;
            fq.CommandTimeout = dbSQLCommandTimeOut;

            return fq;
        }


    }
}