using RabbitMQ.Client;
using TelemetryHubPusherService;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<Worker>();

string hostName = builder.Configuration.GetValue<string>("RabbitMq:HostName");
string user = builder.Configuration.GetValue<string>("RabbitMq:User");
string password = builder.Configuration.GetValue<string>("RabbitMq:Password");

builder.Services.AddSingleton<IConnectionFactory>(sp => new ConnectionFactory
{
    HostName =  hostName,
    UserName = user,
    Password = password
});

var host = builder.Build();
host.Run();
