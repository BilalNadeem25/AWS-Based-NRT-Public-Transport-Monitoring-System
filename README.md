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

An AWS EC2 instance is created to serve as a central compute node for hosting required scripts to run the application. It is also used to provide a shared environment with a single operating system to run the same command line tools for all users.
