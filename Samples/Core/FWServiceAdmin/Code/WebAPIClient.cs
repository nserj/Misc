using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace WebAPI
{
#pragma warning disable // Disable all warnings

    public partial class MSClient
    {
        private string _baseUrl;
        private HttpClient _httpClient;
        private System.Lazy<JsonSerializerSettings> _settings;

        public MSClient(HttpClient httpClient)
        {
            _httpClient = httpClient;
            _settings = new Lazy<JsonSerializerSettings>(() =>
            {
                var settings = new JsonSerializerSettings();
                UpdateJsonSerializerSettings(settings);
                return settings;
            });
        }

        public string BaseUrl
        {
            get { return _baseUrl; }
            set { _baseUrl = value; }
        }

        public JsonSerializerSettings JsonSerializerSettings { get { return _settings.Value; } }

        partial void UpdateJsonSerializerSettings(JsonSerializerSettings settings);
        partial void PrepareRequest(HttpClient client, HttpRequestMessage request, string url);
        partial void PrepareRequest(HttpClient client, HttpRequestMessage request, System.Text.StringBuilder urlBuilder);
        partial void ProcessResponse(HttpClient client, HttpResponseMessage response);

        /// <summary>
        /// Execure GET request
        /// </summary>
        /// <typeparam name="T">Type of returned object</typeparam>
        /// <param name="url">relative URL to API</param>
        /// <returns>T object</returns>
        public async Task<T> GetAsync<T>(string url)
        {
            return await GetAsync<T>(url, CancellationToken.None);
        }

            /// <summary>
            /// Execure GET request
            /// </summary>
            /// <typeparam name="T">Type of returned object</typeparam>
            /// <param name="url">relative URL to API</param>
            /// <param name="cancellationToken">A cancellation token that can be used by other objects or threads to receive notice of cancellation.</param>
            /// <returns>T object</returns>
            public async Task<T> GetAsync<T>(string url, CancellationToken cancellationToken)
        {
            var urlBuilder_ = new System.Text.StringBuilder();
            urlBuilder_.Append(BaseUrl != null ? BaseUrl.TrimEnd('/') : "").Append(url);

            var client_ = _httpClient;
            using (var request_ = new HttpRequestMessage())
            {
                request_.Method = new HttpMethod("GET");
                request_.Headers.Accept.Add(new System.Net.Http.Headers.MediaTypeWithQualityHeaderValue("application/json"));

                PrepareRequest(client_, request_, urlBuilder_);
                var url_ = urlBuilder_.ToString();
                request_.RequestUri = new Uri(url_, System.UriKind.RelativeOrAbsolute);
                PrepareRequest(client_, request_, url_);

                using (var response_ = await client_.SendAsync(request_, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false))
                {
                    var headers_ = Enumerable.ToDictionary(response_.Headers, h_ => h_.Key, h_ => h_.Value);
                    if (response_.Content != null && response_.Content.Headers != null)
                    {
                        foreach (var item_ in response_.Content.Headers)
                            headers_[item_.Key] = item_.Value;
                    }

                    ProcessResponse(client_, response_);

                    var status_ = ((int)response_.StatusCode).ToString();
                    if (status_ == "200")
                    {
                        var responseData_ = response_.Content == null ? null : await response_.Content.ReadAsStringAsync().ConfigureAwait(false);
                        T result_ = (T)default(T);
                        try
                        {
                            result_ = JsonConvert.DeserializeObject<T>(responseData_, _settings.Value);
                            return result_;
                        }
                        catch (System.Exception exception_)
                        {
                            throw new WACException("Could not deserialize the response body.", (int)response_.StatusCode, responseData_, headers_, exception_);
                        }
                    }
                    else
                    if (status_ != "200" && status_ != "204")
                    {
                        var responseData_ = response_.Content == null ? null : await response_.Content.ReadAsStringAsync().ConfigureAwait(false);
                        throw new WACException("The HTTP status code of the response was not expected (" + (int)response_.StatusCode + ").", (int)response_.StatusCode, responseData_, headers_, null);
                    }

                    return default(T);
                }
            }
        }

        /// <summary>
        /// Execute POST request
        /// </summary>
        /// <typeparam name="TPrm">Type of send parameter</typeparam>
        /// <typeparam name="TRet">Type of return object</typeparam>
        /// <param name="url">relative URL to API</param>
        /// <param name="prm">object to post</param>
        /// <returns>TRet object</returns>
        public async Task<TRet> PostAsync<TPrm, TRet>(string url, TPrm prm)
        {
            return await PostAsync<TPrm, TRet>(url,  prm, CancellationToken.None);
        }

        /// <summary>
        /// Execute POST request
        /// </summary>
        /// <typeparam name="TPrm">Type of send parameter</typeparam>
        /// <typeparam name="TRet">Type of return object</typeparam>
        /// <param name="url">relative URL to API</param>
        /// <param name="prm">object to post</param>
        /// <param name="cancellationToken">A cancellation token that can be used by other objects or threads to receive notice of cancellation.</param>
        /// <returns>TRet object</returns>
        public async Task<TRet> PostAsync<TPrm, TRet>(string url, TPrm prm, CancellationToken cancellationToken)
        {
            var urlBuilder_ = new System.Text.StringBuilder();
            urlBuilder_.Append(BaseUrl != null ? BaseUrl.TrimEnd('/') : "").Append(url);

            var client_ = _httpClient;
            using (var request_ = new HttpRequestMessage())
            {
                var content_ = new StringContent(JsonConvert.SerializeObject(prm, _settings.Value));
                content_.Headers.ContentType = System.Net.Http.Headers.MediaTypeHeaderValue.Parse("application/json");
                request_.Content = content_;
                request_.Method = new HttpMethod("POST");
                request_.Headers.Accept.Add(new System.Net.Http.Headers.MediaTypeWithQualityHeaderValue("application/json"));

                PrepareRequest(client_, request_, urlBuilder_);
                var url_ = urlBuilder_.ToString();
                request_.RequestUri = new Uri(url_, System.UriKind.RelativeOrAbsolute);
                PrepareRequest(client_, request_, url_);

                using (var response_ = await client_.SendAsync(request_, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false))
                {
                    var headers_ = Enumerable.ToDictionary(response_.Headers, h_ => h_.Key, h_ => h_.Value);
                    if (response_.Content != null && response_.Content.Headers != null)
                    {
                        foreach (var item_ in response_.Content.Headers)
                            headers_[item_.Key] = item_.Value;
                    }

                    ProcessResponse(client_, response_);

                    var status_ = ((int)response_.StatusCode).ToString();
                    if (status_ == "200")
                    {
                        var responseData_ = response_.Content == null ? null : await response_.Content.ReadAsStringAsync().ConfigureAwait(false);
                        var result_ = default(TRet);
                        try
                        {
                            result_ = JsonConvert.DeserializeObject<TRet>(responseData_, _settings.Value);
                            return result_;
                        }
                        catch (System.Exception exception_)
                        {
                            throw new WACException("Could not deserialize the response body.", (int)response_.StatusCode, responseData_, headers_, exception_);
                        }
                    }
                    else
                    if (status_ != "200" && status_ != "204")
                    {
                        var responseData_ = response_.Content == null ? null : await response_.Content.ReadAsStringAsync().ConfigureAwait(false);
                        throw new WACException("The HTTP status code of the response was not expected (" + (int)response_.StatusCode + ").", (int)response_.StatusCode, responseData_, headers_, null);
                    }

                    return default(TRet);
                }
            }
        }


    }

    #region Exceptions

    public partial class WACException : System.Exception
    {
        public int StatusCode { get; private set; }

        public string Response { get; private set; }

        public Dictionary<string, IEnumerable<string>> Headers { get; private set; }

        public WACException(string message, int statusCode, string response, Dictionary<string, IEnumerable<string>> headers, System.Exception innerException)
            : base(message + "\n\nStatus: " + statusCode + "\nResponse: \n" + response.Substring(0, response.Length >= 512 ? 512 : response.Length), innerException)
        {
            StatusCode = statusCode;
            Response = response;
            Headers = headers;
        }

        public override string ToString()
        {
            return string.Format("HTTP Response: \n\n{0}\n\n{1}", Response, base.ToString());
        }
    }

    public partial class WACException<TResult> : WACException
    {
        public TResult Result { get; private set; }

        public WACException(string message, int statusCode, string response, Dictionary<string, IEnumerable<string>> headers, TResult result, System.Exception innerException)
            : base(message, statusCode, response, headers, innerException)
        {
            Result = result;
        }
    }

    #endregion

}