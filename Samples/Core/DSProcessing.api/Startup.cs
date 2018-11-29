using AutoMapper;
using DSProcessing.api.Common.Attributes;
using DSProcessing.api.Common.Extensions;
using DSProcessing.api.Common.Settings;
using DSProcessing.api.IoC.Configuration.Profiles;
using DSProcessing.api.Swagger;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.ApiExplorer;
using Microsoft.AspNetCore.Mvc.Versioning;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Swashbuckle.AspNetCore.Swagger;
using DSProcessing.api.Services;
using DSProcessing.api.Services.Contracts;
using System.Linq;


namespace DSProcessing.api
{
    public class Startup
    {

        public IConfiguration Configuration { get; private set; }
        public IHostingEnvironment HostingEnvironment { get; private set; }

        private AppSettings _appSettings;

        public Startup(IConfiguration configuration, IHostingEnvironment env)
        {
            HostingEnvironment = env;
            Configuration = configuration;
        }

        public void ConfigureServices(IServiceCollection services)
        {

            services.AddMvc(
                opt => opt.Filters.Add(typeof(CustomFilterAttribute))
                )
                .SetCompatibilityVersion(CompatibilityVersion.Version_2_1);

            var appSettingsSection = Configuration.GetSection("AppSettings");
            if (appSettingsSection == null)
                throw new System.Exception("No appsettings section has been found");

            services.Configure<AppSettings>(appSettingsSection);

            _appSettings = appSettingsSection.Get<AppSettings>();

            if (_appSettings.IsValid())
            {
                if (_appSettings.Swagger.Enabled)
                {


                    services.AddSwaggerGen(c =>
                    {
                        c.SwaggerDoc("v1", new Info { Title = "DSProcessing", Version = "v1" });
                        c.ResolveConflictingActions(apiDescriptions => apiDescriptions.First());
                    });

                }
            }

            services.AddAutoMapper();
            ConfigureMaps();

            services.AddTransient<IDSProcessingService, DSProcessingService>();

        }

        public void Configure(IApplicationBuilder app, IHostingEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }
            else
            {
                app.UseExceptionHandler("/Error");
                app.UseHsts();
            }

            app.UseHttpsRedirection();

            if (_appSettings.IsValid())
            {
                if (_appSettings.Swagger.Enabled)
                {

                    app.UseSwagger();

                    app.UseSwaggerUI(c =>
                    {
                        c.SwaggerEndpoint("/swagger/v1/swagger.json", "DSProcessing");
                    //    c.RoutePrefix = string.Empty; //!!!!!!!!!!!!!!!!!
                    });

                }
            }

            app.UseMvc();
        }

        private void ConfigureMaps()
        {
            Mapper.Initialize(cfg =>
            {
                cfg.AddProfile<APIMappingProfile>();
                cfg.AddProfile<ServicesMappingProfile>();
            }
                );
        }


    }
}
