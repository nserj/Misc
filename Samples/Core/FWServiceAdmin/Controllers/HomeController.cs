using FWServiceAdmin.Code;
using FWServiceAdmin.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using System;
using System.IO;
using Microsoft.AspNetCore.Mvc.ViewEngines;
using Microsoft.AspNetCore.Mvc.ViewFeatures;
using Microsoft.AspNetCore.Mvc.Rendering;

namespace FWServiceAdmin.Controllers
{
    public class HomeController : Controller
    {

        protected Dictionary<string, PropertyValue> dictState;

            private ICompositeViewEngine _viewEngine;

            public HomeController(ICompositeViewEngine viewEngine)
            {
                _viewEngine = viewEngine;
            }


        public async Task<IActionResult> Index()
        {

                 if (ModelState.IsValid )
                 {
                      await GetStatus();
                 }

                 return View(dictState);

        }
        
        private async Task<Dictionary<string, PropertyValue>> GetStatus()
        {
            TaskStateReport st;

            using (HttpClient hcl = new HttpClient())
            {
                WebAPI.WebAPIClient cl = new WebAPI.WebAPIClient(hcl);
                 st= await cl.GetStateAsync();
            }

            dictState= ReflectionTools.DictionaryFromType(st);
            return dictState;

        }


        private async Task<string> RenderPartialViewToString(string viewName, object model)
        {
            if (string.IsNullOrEmpty(viewName))
                viewName = ControllerContext.ActionDescriptor.ActionName;

            ViewData.Model = model;

            using (var writer = new StringWriter())
            {
                ViewEngineResult viewResult =
                    _viewEngine.FindView(ControllerContext, viewName, false);

                ViewContext viewContext = new ViewContext(
                    ControllerContext,
                    viewResult.View,
                    ViewData,
                    TempData,
                    writer,
                    new HtmlHelperOptions()
                );

                await viewResult.View.RenderAsync(viewContext);

                return writer.GetStringBuilder().ToString();
            }
        }


        [HttpPost]
        public async Task<JsonResult> Freeze([FromBody] JObject freeze)
        {

            if (freeze == null)
            {
                return null;
            }

            using (HttpClient hcl = new HttpClient())
            {
                WebAPI.WebAPIClient cl = new WebAPI.WebAPIClient(hcl);

                await cl.FreezeAsync(freeze.GetValue("freeze").Value<bool>());
            }

            await GetStatus();

            return Json (new { state = dictState["Freezed"].Value, pdata = await RenderPartialViewToString("_ServiceStateTablePartial", dictState) });
        }

    }
}
