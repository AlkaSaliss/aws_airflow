import boto3
import json
import configparser
import argparse
import os
import time


config = configparser.ConfigParser()
with open('dwh.cfg') as f:
	config.read_file(f)


def create_iam_role():
	"""Creates an IAM role and saves role's characteristics into 'role_arn.json' file
	"""
	iam = boto3.client("iam",
                    region_name="us-west-2",
                    aws_access_key_id=config.get("AWS", "KEY"),
                    aws_secret_access_key=config.get("AWS", "SECRET")
                    )
	try:
		print("***************** Creating a new IAM Role ***********************")
		_ = iam.create_role(
			Path="/",
			RoleName=config.get("IAM_ROLE", "ROLE_NAME"),
			Description="Allows Redshift Clusters to call AWS on my behalf",
			AssumeRolePolicyDocument=json.dumps({
				"Statement": [{"Action": "sts:AssumeRole", "Effect": "Allow",
                                    "Principal": {"Service": "redshift.amazonaws.com"}
                   }],
				"Version": "2012-10-17"
			})
		)
		print("***************** Attaching Read Policy to Role ***********************")
		iam.attach_role_policy(
			RoleName=config.get("IAM_ROLE", "ROLE_NAME"),
			PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
		)["ResponseMetadata"]["HTTPStatusCode"]
		roleArn = iam.get_role(RoleName=config.get("IAM_ROLE", "ROLE_NAME"))
		roleArn["Role"]["CreateDate"] = str(roleArn["Role"]["CreateDate"])

		print("***************** Saving arn to role_arn.json file ***********************")
		with open("role_arn.json", "w") as f:
			json.dump(roleArn, f)
		print("***************** Done ***********************")
	except Exception as e:
		print(e)


def delete_iam_role():
	"""Deletes IAM role and removes `role_arn.json` file containing the role's characteristics
	"""
	iam = boto3.client("iam",
                    region_name="us-west-2",
                    aws_access_key_id=config.get("AWS", "KEY"),
                    aws_secret_access_key=config.get("AWS", "SECRET")
                    )
	try:
		print("***************** Deleting Role ***********************")
		iam.detach_role_policy(RoleName=config.get(
			"IAM_ROLE", "ROLE_NAME"), PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
		iam.delete_role(RoleName=config.get("IAM_ROLE", "ROLE_NAME"))
		if os.path.isfile("role_arn.json"):
			os.remove("role_arn.json")
		print("***************** Role Deleted ***********************")

	except Exception as e:
		print(e)


def create_redshift_cluster():
	"""Creates an Redshift clusetr and saves cluster's characteristics into 'redshift_cluster.json' file
	"""
	print("***************** Creating redshift python client ***********************")
	redshift = boto3.client("redshift",
                         region_name="us-west-2",
                         aws_access_key_id=config.get("AWS", "KEY"),
                         aws_secret_access_key=config.get("AWS", "SECRET")
                         )
	print("***************** Clients creation Done ***********************")
	print("***************** Creating redshift cluster ***********************")
	try:
		_ = redshift.create_cluster(
			ClusterType=config.get("CLUSTER", "DWH_CLUSTER_TYPE"),
			NodeType=config.get("CLUSTER", "DWH_NODE_TYPE"),
			NumberOfNodes=int(config.get("CLUSTER", "DWH_NUM_NODES")),
			DBName=config.get("CLUSTER", "DB_NAME"),
			ClusterIdentifier=config.get("CLUSTER", "DWH_CLUSTER_IDENTIFIER"),
			MasterUsername=config.get("CLUSTER", "DB_USER"),
			MasterUserPassword=config.get("CLUSTER", "DB_PASSWORD"),
			IamRoles=[config.get("IAM_ROLE", "ARN")]
		)

		status_ok = False
		while not status_ok:
			time.sleep(10)
			cluster_props = redshift.describe_clusters(ClusterIdentifier=config.get("CLUSTER", "DWH_CLUSTER_IDENTIFIER"))['Clusters'][0]
			if cluster_props["ClusterStatus"] == "available":
				status_ok = True
				print("***************** Cluster created ***********************")
		cluster_props_keys = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
		cluster_props = {k: v for k, v in cluster_props.items() if k in cluster_props_keys}

		print("***************** Saving cluster props to redshift_cluster.json ***********************")
		with open("redshift_cluster.json", "w") as f:
			json.dump(cluster_props, f)
		
		print("***************** Done ***********************")

	except Exception as e:
		print(e)


def delete_redshift_cluster():
	"""Deletes Redshift cluster and removes `redshift_cluster.json` file containing the clusters's characteristics
	"""
	print("***************** Deleting redshift Cluster ***********************")
	redshift = boto3.client("redshift",
                         region_name="us-west-2",
                         aws_access_key_id=config.get("AWS", "KEY"),
                         aws_secret_access_key=config.get("AWS", "SECRET")
                         )
	try:
		redshift.delete_cluster(ClusterIdentifier=config.get("CLUSTER", "DWH_CLUSTER_IDENTIFIER"),  SkipFinalClusterSnapshot=True)
		print("***************** Cluster deleted ***********************")
	except Exception as e:
		print(e)


if __name__ == "__main__":
	parser = argparse.ArgumentParser(
		description="Helper script for interacting ith AWS Infra")
	parser.add_argument("-t", "--task", help="Type of task",
	                    required=True, choices=["create_role", "delete_role", "create_cluster", "delete_cluster"])
	args = parser.parse_args()

	if args.task == "create_role":
		create_iam_role()
	elif args.task == "delete_role":
		delete_iam_role()
	elif args.task == "create_cluster":
		create_redshift_cluster()
	elif args.task == "delete_cluster":
		delete_redshift_cluster()