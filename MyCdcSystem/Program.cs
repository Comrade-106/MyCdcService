using MyCdcSystem.Core.Pipeline;
using MyCdcSystem.Core.StateSystem;
using MyCdcSystem.Services;
using Prometheus;
using Prometheus.DotNetRuntime;

var builder = WebApplication.CreateBuilder(args);

builder.Logging.ClearProviders(); // Очищаем текущие провайдеры
builder.Logging.AddConsole(); // Добавляем логирование в консоль
builder.Logging.AddDebug(); // Логирование в Debug
builder.Logging.AddEventSourceLogger(); // Для отслеживания событий в EventSource

// Add services to the container.
builder.Services.AddScoped<StateStore>();
builder.Services.AddScoped<StateManager>();
builder.Services.AddSingleton<ChangeDataCaptureService>();
builder.Services.AddScoped<PipelineBuilder>();

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
}
    app.UseSwagger();
    app.UseSwaggerUI();

var collector = DotNetRuntimeStatsBuilder.Default().StartCollecting();

app.UseHttpsRedirection();

app.UseRouting();

app.UseAuthorization();

app.UseEndpoints(endpoints =>
{
    endpoints.MapControllers();
    endpoints.MapMetrics();
});
Console.WriteLine("Test");
app.Run();
