using Amazon.Extensions.NETCore.Setup;
using Amazon.S3;
using DP2SaaSMCS.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NJsonSchema;
using NSwag.AspNetCore;
using ScheduledService.Code;
using ScheduledService.Code.Scheduling;

namespace DP2SaaSMCS
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        public void ConfigureServices(IServiceCollection services)
        {
            services.AddMvc().SetCompatibilityVersion(CompatibilityVersion.Version_2_1);

            services.AddSingleton<IMCSException, MCSException>();
            services.AddSingleton<ScheduledTaskState, TaskState>();
            services.AddSingleton<IScheduledTask, CurrentTask>();
            services.AddScheduler((sendern, args) =>
            {
                NLog.LogManager.GetCurrentClassLogger().Error(args.Exception, "Unobserved Task Exception");
                args.SetObserved();
            });

            AWSOptions op = Configuration.GetAWSOptions();
            services.AddDefaultAWSOptions(op);
            services.AddAWSService<IAmazonS3>(op);


            services.AddSwagger();
        }

        public void Configure(IApplicationBuilder app, IHostingEnvironment env)
        {

            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }
            else
            {
                app.UseHsts();
            }

            app.UseHttpsRedirection();
            app.UseMvc();

            app.UseSwaggerUi3WithApiExplorer(settings =>
            {
                settings.GeneratorSettings.DefaultPropertyNameHandling =
                    PropertyNameHandling.CamelCase;
            });
        }
    }
}
