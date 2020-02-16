using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using AutoMapper;

using Pulumi;
using Pulumi.Aws.Ec2.Inputs;

using iam = Pulumi.Aws.Iam;
using ec2 = Pulumi.Aws.Ec2;
using redshift = Pulumi.Aws.RedShift;
using ssm = Pulumi.Aws.Ssm;

public class DendConfiguration
{
    public string Region { get; set; }
    public string PublicIp { get; set; }
    public string VpcId { get; set; }

    public int Port { get; set; }
}
public class DendConfigurationDTO
{
    public string Region { get; set; }
    public string PublicIp { get; set; }
    public string VpcId { get; set; }
}

public class Infrastructure 
{
    
    public CustomResourceOptions CustomResourceOptions { get; set; }
    public InvokeOptions InvokeOptions { get; set; }

    public DendConfiguration Configuration { get; }

    public Infrastructure(DendConfigurationDTO configurationDto) {
        var config = new MapperConfiguration(cfg =>  cfg.CreateMap<DendConfigurationDTO, DendConfiguration>());
        var mapper = config.CreateMapper();
        this.Configuration = mapper.Map<DendConfigurationDTO, DendConfiguration>(configurationDto);
        this.Configuration.Port = 5439;
    }

    private iam.Role createRedshiftRole() 
    {
        var redShiftRole = new iam.Role("dend-redshift-role", new iam.RoleArgs
            {
                Description = "Role Created for Udacity's DEND",
                Path = "/service-role/redshift.amazonaws.com/dend/",
                AssumeRolePolicy = @"{
                    ""Version"": ""2012-10-17"",
                    ""Statement"": [
                        {
                            ""Sid"": """",
                            ""Effect"": ""Allow"",
                            ""Principal"": {
                                ""Service"": [
                                    ""redshift.amazonaws.com""
                                ]
                            },
                            ""Action"": ""sts:AssumeRole""
                        }
                    ]
                }",
            }, this.CustomResourceOptions);

        var redShiftRolePolicyAttachment = new iam.RolePolicyAttachment("redshift-s3-readonly-attachment", new iam.RolePolicyAttachmentArgs
            {
                Role = redShiftRole.Name,
                PolicyArn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
            }, this.CustomResourceOptions);

        
        return redShiftRole;
    }

    private ec2.SecurityGroup createRedshiftSecurityGroup(string vpcId, string localPublicIp, int port) {
        var redshiftSG = new ec2.SecurityGroup("redshift_security_group", new ec2.SecurityGroupArgs
            {
                VpcId = vpcId,
                Description = "Auth Redshift Cluster to connect",
                Ingress =
                {
                    new SecurityGroupIngressArgs
                    {
                        Protocol = "tcp",
                        FromPort = port,
                        ToPort = port,
                        CidrBlocks = { localPublicIp }
                    }
                }
            }, this.CustomResourceOptions);

        return redshiftSG;
    }

    private async Task<string> getRedshiftPassword() {
        var password = await ssm.Invokes.GetParameter(new ssm.GetParameterArgs 
            {
                Name = "redshift-cluster",
                WithDecryption = true
            });
        return password.Value;
    }

    private async Task<redshift.Cluster> createRedshiftCluster(int port, ec2.SecurityGroup securityGroup, iam.Role role) {
        var password = await getRedshiftPassword();
        var cluster = new redshift.Cluster("dend-redshift-cluster", new redshift.ClusterArgs 
            {
                NodeType = "dc2.large",
                NumberOfNodes = 2,
                ClusterIdentifier = "dend-redshift-cluster",
                DatabaseName = "dev",
                Port = port,
                MasterUsername = "awsuser",
                MasterPassword = password,
                VpcSecurityGroupIds = { securityGroup.Id },
                IamRoles = { "AWSServiceRoleForRedshift", role.Id },
                FinalSnapshotIdentifier = "dend-snapshot"
            }, this.CustomResourceOptions);
        return cluster;
    }

    public Task<int> runDeployment() {
        return Deployment.RunAsync(async () =>
        {
            var provider = new Pulumi.Aws.Provider(String.Format("provider-{0}", Configuration.Region), new Pulumi.Aws.ProviderArgs{
                Region = Configuration.Region
            });
            this.CustomResourceOptions = new CustomResourceOptions {
                Provider = provider
            };
            this.InvokeOptions = new InvokeOptions {
                Provider = provider
            };
            var role = createRedshiftRole();
            var vpc = await ec2.Invokes.GetVpc(new ec2.GetVpcArgs { Id = this.Configuration.VpcId }, this.InvokeOptions);
            var securityGroup = createRedshiftSecurityGroup(vpc.Id, this.Configuration.PublicIp, this.Configuration.Port);
            var cluster = await createRedshiftCluster(this.Configuration.Port, securityGroup, role);

            return new Dictionary<string, object?>
            {
                {   "role", role.Name                   },
                {   "securityGroup", securityGroup.Id   },
                {   "cluster", cluster.Id               }
            };
        });
    }
}