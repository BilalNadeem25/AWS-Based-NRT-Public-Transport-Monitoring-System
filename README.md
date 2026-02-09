# AWS-Based-NRT-Public-Transport-Monitoring-System

## Introduction
Public transport systems increasingly rely on near real-time data to improve service reliability and provide passengers with accurate transport arriving times and schedule information to improve commute efficiency. The General Transit Feed Specification Real-Time (GTFS-RT)  API allows transit agencies to share real-time updates such as vehicle locations, trip and timing changes, or any alerts on services. This project focuses on designing and deploying a distributed, cloud-based monitoring system that leverages GTFS near real time feeds at a large scale and processes the data in near real-time using AWS managed services.

## System Architecture
<p align="center">
  <img src="images/architecture.jpeg" width="1000">
</p>

## Pre-requisites
A shared AWS environment was configured to support collaborative development of a near real-time bus monitoring dashboard. The following setups focus on secure authentication, shared access to compute, and a common workspace:

### 1. EC2 Instance
<p align="center">
  <img src="images/ec2-instance.png" width="1000">
</p>

An AWS EC2 instance is created to serve as a central compute node for hosting required scripts to run the application. It is also used to provide a shared environment with a single operating system to run the same command line tools for all users. It is configured with an Amazon Linux AMI for a linux based operating system and a t3 micro instance type. An IAM LabRole is attached to the instance to provide users of the instance access to AWS services such as creating EMR clusters, setup S3, etc. A security group is configured with inbound rules to permit different kinds of network traffic into the system. Port 22 is opened for remote access from specific IP addresses into the EC2 using SSH and port 8501 is opened to allow members to access the Streamlit dashboard using a public URL. To access the EC2 instance as the admin, the following steps are implemented:

<details>
  <summary><strong>Accessing the EC2 Instance</strong></summary>

  1. When launching the EC2 instance from the AWS Management Console, generate a SSH key pair which will automatically download.
  2. The following command was used to login to the instance using the downloaded SSH key pair file.
     ```
     ssh -i <ssh-key.pem> ec2-user@<public IP of instance>
     ```
  3. After logging in, users are created for group members using the following command:
     ```
     sudo adduser <username>
     ```
  4. Each member generates a SSH key-pair using RSA algorithm and provide the public key to admin.
  5. An authorized keys file is created for each member to store their respective SSH public keys and provide read write permissions using the following command:
     ```
     sudo mkdir /home/<username>/.ssh
     sudo nano /home/<username>/.ssh/authorized_keys
     sudo chown -R <username>:<username> /home/<username>/.ssh
     sudo chmod 700 /home/<username>/.ssh
     sudo chmod 600 /home/<username>/.ssh/authorized_keys
     ```
</details>

### 2. Shared Project Workspace



