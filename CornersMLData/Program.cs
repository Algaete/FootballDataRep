using Microsoft.OpenApi.Models;
using System.IO;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddControllers();

// Swagger / OpenAPI (Swashbuckle)
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(options =>
{
    options.SwaggerDoc("v1", new OpenApiInfo { Title = "CornersMLData API", Version = "v1" });

    var xmlFile = $"{System.Reflection.Assembly.GetExecutingAssembly().GetName().Name}.xml";
    var xmlPath = Path.Combine(AppContext.BaseDirectory, xmlFile);
    if (File.Exists(xmlPath)) options.IncludeXmlComments(xmlPath);
});

var app = builder.Build();

// Serve static files (keep for openapi.json if present)
app.UseDefaultFiles();
app.UseStaticFiles();

// Configure the HTTP request pipeline.
app.UseSwagger();
app.UseSwaggerUI(c => c.SwaggerEndpoint("/swagger/v1/swagger.json", "CornersMLData v1"));

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

// try to open browser to swagger after a small delay (best-effort)
try
{
    var urlsToTry = new[] { "https://localhost:7125/swagger", "http://localhost:5175/swagger", "https://localhost:5001/swagger", "http://localhost:5000/swagger" };
    _ = Task.Run(async () =>
    {
        await Task.Delay(1200);
        foreach (var u in urlsToTry)
        {
            try
            {
                var psi = new System.Diagnostics.ProcessStartInfo { FileName = u, UseShellExecute = true };
                System.Diagnostics.Process.Start(psi);
                break;
            }
            catch
            {
                // ignore
            }
        }
    });
}
catch
{
}

app.Run();

