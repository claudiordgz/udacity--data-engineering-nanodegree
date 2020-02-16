using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using System;
using System.IO;

class Program
{
    static Task<int> Main()
    {
        var Config = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json")
                .Build();
        var dendEnvironmentConfiguration = new DendConfigurationDTO();
        dendEnvironmentConfiguration.VpcId = Config["vpcId"];
        dendEnvironmentConfiguration.PublicIp = Config["publicIp"];
        dendEnvironmentConfiguration.Region = Config["region"];
        var infrastructure = new Infrastructure(dendEnvironmentConfiguration);
        return infrastructure.runDeployment();
    }
}
