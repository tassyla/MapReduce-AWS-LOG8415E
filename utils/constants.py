import os


KEY_NAME = "labsuser"
DEFAULT_KEY_PATH = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", f"labsuser.pem")
)


# ===== LEGACY (Assignment 1) =====
# APP_PORT = 8000
# AMI_PARAM = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
# CLUSTER1_COUNT = 4
# CLUSTER1_TYPE = "t2.large"
# CLUSTER2_COUNT = 4
# CLUSTER2_TYPE = "t2.micro"
# LB_INSTANCE_NAME = "custom-software-lb"
# LB_INSTANCE_TYPE = "t2.micro"
# DEFAULT_USER = "ec2-user"
# DEFAULT_CPU_THRESHOLD = 70.0
# DEFAULT_PORT = APP_PORT
# LB_SG_NAME = "custom-lb-sg" # Re-use from Assignment 1


