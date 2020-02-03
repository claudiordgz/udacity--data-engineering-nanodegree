using System.Collections.Generic;
using System.Threading.Tasks;

using Pulumi;
using Pulumi.Aws.Ec2.Inputs;

using iam = Pulumi.Aws.Iam;
using ec2 = Pulumi.Aws.Ec2;
using redshift = Pulumi.Aws.RedShift;
using ssm = Pulumi.Aws.Ssm;

class Program
{

    static iam.Role createRedshiftRole() 
    {
        var redShiftRole = new iam.Role("dend-redshift-role", new iam.RoleArgs
            {
                Description = "Role Created for Udacity's DEND",
                AssumeRolePolicy = @"{
                    ""Version"": ""2008-10-17"",
                    ""Statement"": [{
                        ""Sid"": """",
                        ""Effect"": ""Allow"",
                        ""Principal"": {
                            ""Service"": ""redshift.amazonaws.com""
                        },
                        ""Action"": ""sts:AssumeRole""
                    }]
                }",
            });
        var redShiftRolePolicyAttachment = new iam.RolePolicyAttachment("redshift-policy-attachment", new iam.RolePolicyAttachmentArgs
            {
                Role = redShiftRole.Name,
                PolicyArn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
            });
        
        return redShiftRole;
    }

    static ec2.SecurityGroup createRedshiftSecurityGroup(string vpcId, string localPublicIp, int port) {
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
            });

        return redshiftSG;
    }

    static async Task<string> getRedshiftPassword() {
        var password = await ssm.Invokes.GetParameter(new ssm.GetParameterArgs 
            {
                Name = "redshift-cluster",
                WithDecryption = true
            });
        return password.Value;
    }

    static async Task<redshift.Cluster> createRedshiftCluster(int port, ec2.SecurityGroup securityGroup, iam.Role role) {
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
                IamRoles = { role.Id },
                FinalSnapshotIdentifier = "dend-snapshot"
            });
        return cluster;
    }

    static Task<int> runDeployment() {
        return Deployment.RunAsync(async () =>
        {
            var role = createRedshiftRole();
            var vpc = await ec2.Invokes.GetVpc(new ec2.GetVpcArgs { Id = "vpc-0f77848f3e00211d6" });
            int port = 5439;
            var securityGroup = createRedshiftSecurityGroup(vpc.Id, "67.170.46.98/32", port);
            var cluster = await createRedshiftCluster(port, securityGroup, role);

            return new Dictionary<string, object?>
            {
                {   "role", role.Name                   },
                {   "securityGroup", securityGroup.Id   },
                {   "cluster", cluster.Id               }
            };
        });
    }
    static Task<int> Main()
    {
        return runDeployment();
    }
}
