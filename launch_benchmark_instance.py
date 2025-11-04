from utils.config import make_session
from utils.constants import KEY_NAME, LB_SG_NAME
from urllib.request import urlopen
from benchmark.user_data import build_benchmark_user_data


UBUNTU_SSM_AMI_PARAMS = [
    "/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp3/ami-id",
    "/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id",
    "/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id",
    "/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp2/ami-id",
    "/aws/service/canonical/ubuntu/server/20.04/stable/current/amd64/hvm/ebs-gp3/ami-id",
    "/aws/service/canonical/ubuntu/server/20.04/stable/current/amd64/hvm/ebs-gp2/ami-id",
]


def get_default_vpc_id(ec2_client) -> str:
    vpcs = ec2_client.describe_vpcs(Filters=[{"Name": "is-default", "Values": ["true"]}])["Vpcs"]
    return vpcs[0]["VpcId"]


def get_any_public_subnet(ec2_client, vpc_id: str) -> str:
    subnets = ec2_client.describe_subnets(
        Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "map-public-ip-on-launch", "Values": ["true"]},
        ]
    )["Subnets"]
    return subnets[0]["SubnetId"]


def ensure_benchmark_sg(ec2_client, vpc_id: str) -> str:
    """Ensure a SG exists for the benchmark instance with ports 22 and 80 open"""
    try:
        sg = ec2_client.create_security_group(
            Description="Security group for MapReduce benchmark instance",
            GroupName=LB_SG_NAME,
            VpcId=vpc_id,
        )
        sg_id = sg["GroupId"]
    except ec2_client.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "InvalidGroup.Duplicate":
            resp = ec2_client.describe_security_groups(GroupNames=[LB_SG_NAME])
            sg_id = resp["SecurityGroups"][0]["GroupId"]


    # HTTP 80 from anywhere
    try:
        ec2_client.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[
                {
                    "FromPort": 80,
                    "ToPort": 80,
                    "IpProtocol": "tcp",
                    "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                }
            ],
        )
    except ec2_client.exceptions.ClientError as e:
        if e.response["Error"]["Code"] != "InvalidPermission.Duplicate":
            raise

    # SSH 22 from caller public IP only
    try:
        my_public_ip = urlopen("https://checkip.amazonaws.com").read().decode().strip()
        cidr_ip = f"{my_public_ip}/32"
        ec2_client.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[
                {
                    "FromPort": 22,
                    "ToPort": 22,
                    "IpProtocol": "tcp",
                    "IpRanges": [{"CidrIp": cidr_ip}],
                }
            ],
        )
    except ec2_client.exceptions.ClientError as e:
        if e.response["Error"].get("Code") != "InvalidPermission.Duplicate":
            raise

    return sg_id


def get_ubuntu_ami_id(ssm_client, ec2_client) -> str:
    """Retrieve a recent Ubuntu LTS AMI.

    Strategy:
    1) Try known public SSM parameters (gp3/gp2, 22.04 preferred; 24.04/20.04 as fallback).
    2) If none exist in region, query EC2 images owned by Canonical and pick latest Jammy 22.04.
    """
    # Step 1: Try SSM parameters
    for param in UBUNTU_SSM_AMI_PARAMS:
        try:
            value = ssm_client.get_parameter(Name=param)["Parameter"]["Value"]
            if value and value.startswith("ami-"):
                return value
        except ssm_client.exceptions.ParameterNotFound:
            continue
        except Exception:
            # Ignore unexpected issues and try next option
            continue

    # Step 2: Describe images from Canonical and choose the newest 22.04
    try:
        images = ec2_client.describe_images(
            Owners=["099720109477"],  # Canonical
            Filters=[
                {"Name": "name", "Values": ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]},
                {"Name": "architecture", "Values": ["x86_64"]},
                {"Name": "root-device-type", "Values": ["ebs"]},
                {"Name": "virtualization-type", "Values": ["hvm"]},
            ],
        )["Images"]
        if not images:
            raise RuntimeError("No Ubuntu 22.04 AMIs found from Canonical in this region.")
        images.sort(key=lambda img: img["CreationDate"], reverse=True)
        return images[0]["ImageId"]
    except Exception as e:
        raise SystemExit(
            f"ERROR: Unable to resolve an Ubuntu AMI in this region via SSM or image search: {e}"
        )


def launch_benchmark_instance(ec2_client, ssm_client, subnet_id: str, sg_id: str, instance_type: str = "t2.large") -> str:
    ami_id = get_ubuntu_ami_id(ssm_client, ec2_client)
    user_data_script = build_benchmark_user_data()

    resp = ec2_client.run_instances(
        ImageId=ami_id,
        InstanceType=instance_type,
        MinCount=1,
        MaxCount=1,
        KeyName=KEY_NAME,
        InstanceInitiatedShutdownBehavior="stop",
        SecurityGroupIds=[sg_id],
        SubnetId=subnet_id,
        UserData=user_data_script,
        TagSpecifications=[
            {
                "ResourceType": "instance",
                "Tags": [{"Key": "Name", "Value": "mapreduce-benchmark"}],
            }
        ],
    )
    return resp["Instances"][0]["InstanceId"]


def main():
    sess = make_session()
    ec2_client = sess.client("ec2")
    ssm_client = sess.client("ssm")

    vpc_id = get_default_vpc_id(ec2_client)
    subnet_id = get_any_public_subnet(ec2_client, vpc_id)
    sg_id = ensure_benchmark_sg(ec2_client, vpc_id)

    instance_id = launch_benchmark_instance(ec2_client, ssm_client, subnet_id, sg_id)

    waiter = ec2_client.get_waiter("instance_running")
    try:
        waiter.wait(InstanceIds=[instance_id])
    except Exception as e:
        print(f"Failed to wait for instance to start: {e}")
        raise

    resp = ec2_client.describe_instances(InstanceIds=[instance_id])
    public_ip = resp["Reservations"][0]["Instances"][0].get("PublicIpAddress")

    print("Benchmark instance launched!")
    print(f"Instance ID: {instance_id}")
    print(f"Public IP: {public_ip}")


if __name__ == "__main__":
    main()
