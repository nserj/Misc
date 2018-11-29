using AutoMapper;
using DSProcessing.api.DataContracts;
using DSProcessing.api.Services.Contracts;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Threading.Tasks;
using S = DSProcessing.api.Services.Model;
using DSProcessing.api.DataContracts.Responses;
using DSProcessing.api.DataContracts.Requests;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

namespace DSProcessing.api.API.Controllers
{
    // [ApiVersion("1.0")]
    [Route("api/dsprocessing")]
  //  [Route("api/v{version:apiVersion}/dsprocessing")]
    public class DSProcessingController : Controller
    {
        private readonly IDSProcessingService _service;
        private readonly IMapper _mapper;
        private static NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();

        public DSProcessingController(IDSProcessingService service, IMapper mapper)
        {
            _service = service;
            _mapper = mapper;
        }

        #region GET
        [HttpGet()]
        [Route("GetDSProcessResult")]
        public async Task<IActionResult> GetDSProcessResult([FromHeader] string oid)
        {

            if (string.IsNullOrWhiteSpace(oid))
                return BadRequest();

            Identifier idf;

            try
            {
                idf = JsonConvert.DeserializeObject<Identifier>(oid);
            }
            catch (Exception ex)
            {
                logger.Error(ex, "Json deserialization: {0}", oid);
                return BadRequest();
            }


            if (idf == null || (idf.ID <= 0 && string.IsNullOrWhiteSpace(idf.UID)))
            {
                return BadRequest();
            }

            S.DsDescription res = await _service.GetDSProcessResultAsync(Mapper.Map<S.Identifier>(idf));

            return Ok(Mapper.Map<DsDataCreationResponse>(res));

        }

        #endregion

        #region POST
        [HttpPost]
        [Route("UploadDS")]
        public async Task<IActionResult> UploadDS([FromBody]DSDataCreationRequest data)
        {

            if (data == null || data.Data == null || data.Data.Rows.Length == 0 || data.Data.Columns.Length == 0 || string.IsNullOrWhiteSpace(data.Data.Name))
                return BadRequest();

            long new_id = await _service.UploadDSAsync(Mapper.Map<S.DsDescription>(data));

            return Ok(new_id);
        }

        #endregion

    }
}
