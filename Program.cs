using RabbitMQ.Client;
using TelemetryHubPusherService;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<Worker>();

string hostName = builder.Configuration.GetValue<string>("RabbitMq:HostName") ?? throw new NullReferenceException("Does not found hostname");
string user = builder.Configuration.GetValue<string>("RabbitMq:User") ?? throw new NullReferenceException("Does not found rabbitMq user");
string password = builder.Configuration.GetValue<string>("RabbitMq:Password") ?? throw new NullReferenceException("rabbitMq password is null");

builder.Services.AddSingleton<IConnectionFactory>(sp => new ConnectionFactory
{
    HostName =  hostName,
    UserName = user,
    Password = password
});

var host = builder.Build();
host.Run();
