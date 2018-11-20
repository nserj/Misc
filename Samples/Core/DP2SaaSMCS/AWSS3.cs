using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.Util;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace DP2SaaSMCS.S3
{
    public class AWSS3
    {

        private IAmazonS3 s3Client;
        private string bucket;
        private string key;

        public AWSS3(IAmazonS3 client)
        {
            s3Client = client;
        }

        public AWSS3(IAmazonS3 _client, string _bucket, string _key)
        {

            s3Client = _client ?? throw new ArgumentNullException("s3Client");

            if (string.IsNullOrEmpty(_bucket))
            {
                throw new ArgumentNullException("bucket");
            }

            if (string.IsNullOrEmpty(_key) || string.Equals(_key, "\\"))
            {
                throw new ArgumentNullException("key");
            }

            if (_key.EndsWith("\\", StringComparison.Ordinal))
            {
                throw new ArgumentException("key is a directory name");
            }

            bucket = _bucket;
            key = _key;
        }

        /// <summary>
        /// Deletes the from S3.
        /// </summary>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        public async Task DeleteFileAsync(CancellationToken token = default(CancellationToken))
        {
            var deleteObjectRequest = new DeleteObjectRequest
            {
                BucketName = bucket,
                Key = S3Helper.EncodeKey(key)
            };
            ((Amazon.Runtime.Internal.IAmazonWebServiceRequest)deleteObjectRequest).AddBeforeRequestHandler(S3Helper.FileIORequestEventHandler);
            await s3Client.DeleteObjectAsync(deleteObjectRequest, token);
        }

        /// <summary>
        /// Checks S3 if the file exists in S3 and return true if it does.
        /// </summary>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>

        public async Task<GetObjectMetadataResponse> FileMetadataAsync(CancellationToken token = default(CancellationToken))
        {
            try
            {
                var request = new GetObjectMetadataRequest
                {
                    BucketName = bucket,
                    Key = S3Helper.EncodeKey(key)
                };

                ((Amazon.Runtime.Internal.IAmazonWebServiceRequest)request).AddBeforeRequestHandler(S3Helper.FileIORequestEventHandler);

                // If the object doesn't exist then a "NotFound" will be thrown
                GetObjectMetadataResponse s= await s3Client.GetObjectMetadataAsync(request,token);

                return s;
            }
            catch (AmazonS3Exception e)
            {
                if (string.Equals(e.ErrorCode, "NoSuchBucket"))
                {
                    return null;
                }
                else if (string.Equals(e.ErrorCode, "NotFound"))
                {
                    return null;
                }
                throw;
            }
            catch (OperationCanceledException ex)
            {
                if (ex.CancellationToken != token)
                    throw;

                return null;
            }

        }
    }

    internal static class S3Helper
    {
        internal static string EncodeKey(string key)
        {
            return key.Replace('\\', '/');
        }
        internal static string DecodeKey(string key)
        {
            return key.Replace('/', '\\');
        }

        internal static void FileIORequestEventHandler(object sender, RequestEventArgs args)
        {
            WebServiceRequestEventArgs wsArgs = args as WebServiceRequestEventArgs;
            if (wsArgs != null)
            {
                string currentUserAgent = wsArgs.Headers[AWSSDKUtils.UserAgentHeader];
                wsArgs.Headers[AWSSDKUtils.UserAgentHeader] = currentUserAgent + " FileIO";
            }
        }

    }
}
