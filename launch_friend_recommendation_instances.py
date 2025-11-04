from utils.config import make_session
from utils.constants import KEY_NAME, LB_SG_NAME  # reuse SG name constant
from urllib.request import urlopen
import json
import argparse
import time

UBUNTU_SSM_AMI_PARAMS = [
    "/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp3/ami-id",
    "/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id",
    "/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id",
    "/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp2/ami-id",
    "/aws/service/canonical/ubuntu/server/20.04/stable/current/amd64/hvm/ebs-gp3/ami-id",
    "/aws/service/canonical/ubuntu/server/20.04/stable/current/amd64/hvm/ebs-gp2/ami-id",
]

ROLES = ["mapper1", "mapper2", "reducer"]

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

def ensure_s3_bucket(s3_client, bucket: str, region: str):
    if not bucket:
        return
    try:
        s3_client.head_bucket(Bucket=bucket)
        print(f"[S3] Bucket exists: {bucket}")
        return
    except Exception:
        pass
    print(f"[S3] Creating bucket: {bucket}")
    if region == "us-east-1":
        s3_client.create_bucket(Bucket=bucket)
    else:
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": region},
        )

def ensure_friend_sg(ec2_client, vpc_id: str, group_name: str) -> str:
    try:
        sg = ec2_client.create_security_group(
            Description="Security group for friend-recommendation mappers/reducer",
            GroupName=group_name,
            VpcId=vpc_id,
        )
        sg_id = sg["GroupId"]
    except ec2_client.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "InvalidGroup.Duplicate":
            resp = ec2_client.describe_security_groups(GroupNames=[group_name])
            sg_id = resp["SecurityGroups"][0]["GroupId"]
        else:
            raise

    # HTTP
    try:
        ec2_client.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[{
                "FromPort": 80, "ToPort": 80, "IpProtocol": "tcp",
                "IpRanges": [{"CidrIp": "0.0.0.0/0"}]
            }],
        )
    except ec2_client.exceptions.ClientError as e:
        if e.response["Error"]["Code"] != "InvalidPermission.Duplicate":
            raise

    # SSH
    try:
        my_public_ip = urlopen("https://checkip.amazonaws.com", timeout=5).read().decode().strip()
        cidr_ip = f"{my_public_ip}/32"
        ec2_client.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[{
                "FromPort": 22, "ToPort": 22, "IpProtocol": "tcp",
                "IpRanges": [{"CidrIp": cidr_ip}]
            }],
        )
    except ec2_client.exceptions.ClientError as e:
        if e.response["Error"].get("Code") != "InvalidPermission.Duplicate":
            raise

    return sg_id

def get_ubuntu_ami_id(ssm_client, ec2_client) -> str:
    for param in UBUNTU_SSM_AMI_PARAMS:
        try:
            value = ssm_client.get_parameter(Name=param)["Parameter"]["Value"]
            if value and value.startswith("ami-"):
                return value
        except Exception:
            continue
    # Fallback: latest 22.04 Jammy from Canonical
    images = ec2_client.describe_images(
        Owners=["099720109477"],
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

def launch_instance(ec2_client, ami_id: str, subnet_id: str, sg_id: str, role: str, instance_type: str):
    resp = ec2_client.run_instances(
        ImageId=ami_id,
        InstanceType=instance_type,
        MinCount=1, MaxCount=1,
        KeyName=KEY_NAME,
        SecurityGroupIds=[sg_id],
        SubnetId=subnet_id,
        TagSpecifications=[{
            "ResourceType": "instance",
            "Tags": [{"Key": "Name", "Value": role}, {"Key": "Role", "Value": role}, {"Key": "Project", "Value": "friend-recommendation"}],
        }],
    )
    return resp["Instances"][0]["InstanceId"]

def wait_public_ip(ec2_client, instance_id: str) -> str:
    waiter = ec2_client.get_waiter("instance_running")
    waiter.wait(InstanceIds=[instance_id])
    ip = None
    for _ in range(60):
        d = ec2_client.describe_instances(InstanceIds=[instance_id])
        i = d["Reservations"][0]["Instances"][0]
        ip = i.get("PublicIpAddress")
        if ip:
            return ip
        time.sleep(2)
    raise RuntimeError(f"Instance {instance_id} has no public IP after waiting.")

def main():
    ap = argparse.ArgumentParser(description="Launch 3 instances for friend recommendation (mapper1, mapper2, reducer)")
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--bucket", default="", help="Optional: ensure S3 bucket exists (name must be globally unique)")
    ap.add_argument("--instance-type", default="t2.micro")
    args = ap.parse_args()

    sess = make_session()
    ec2_client = sess.client("ec2", region_name=args.region)
    ssm_client = sess.client("ssm", region_name=args.region)
    s3_client  = sess.client("s3",  region_name=args.region)

    if args.bucket:
        ensure_s3_bucket(s3_client, args.bucket, args.region)

    vpc_id    = get_default_vpc_id(ec2_client)
    subnet_id = get_any_public_subnet(ec2_client, vpc_id)
    sg_id     = ensure_friend_sg(ec2_client, vpc_id, LB_SG_NAME)

    ami_id = get_ubuntu_ami_id(ssm_client, ec2_client)

    instance_ids = {}
    public_ips   = {}
    for role in ROLES:
        iid = launch_instance(ec2_client, ami_id, subnet_id, sg_id, role, args.instance_type)
        instance_ids[role] = iid

    for role, iid in instance_ids.items():
        public_ips[role] = wait_public_ip(ec2_client, iid)

    summary = {
        "region": args.region,
        "bucket": args.bucket,
        "security_group": sg_id,
        "instances": instance_ids,
        "public_ips": public_ips,
    }
    print(json.dumps(summary, indent=2))

    print("EXPORTS FOR BASH")
    for role, ip in public_ips.items():
        print(f'export {role.upper()}_IP="{ip}"')
    print(f'export FRIEND_BUCKET="{args.bucket}"')

if __name__ == "__main__":
    main()
