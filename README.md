# KafkaManager

KafkaManager is a tool for managing Apache Kafka topics, access control lists (ACLs), and connectors. It leverages the Kafka REST API to perform actions such as creating and updating topics, managing ACLs, and configuring connectors.

## Table of Contents

- [Features](#features)
- [Getting Started](#getting-started)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Running KafkaManager](#running-kafkamanager)
- [Automated Resource Management](#automated-resource-management)
- [Contributing](#contributing)
- [License](#license)
- [Acknowledgments](#acknowledgments)

## Features

- **Topic Management:** Create, update, and delete Kafka topics.
- **ACL Management:** Add or remove access control lists for topics.
- **Connector Configuration:** Manage Kafka connectors by adding or removing them.
- **Automated Resource Management:** Automatically apply changes based on git diffs.

## Getting Started

### Prerequisites

- Python 3.x
- Git
- Kafka Cluster with REST Proxy and Connect configurations
- Access to Kafka REST API with appropriate credentials
- Access to Connect REST API with appropriate credentials

### Installation

1. Clone the repository:

```bash
git clone https://github.com/your-username/kafkamanager.git
cd kafkamanager
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

### Configuration

1. Set the necessary environment variables:

```bash
export GITHUB_TOKEN=your_github_token
export REST_URL=your_kafka_rest_proxy_url
export KAFKA_CLUSTER_ID=your_kafka_cluster_id
export CONNECT_REST_URL=your_kafka_connect_rest_url
export REST_BASIC_AUTH_USER=your_kafka_rest_basic_auth_user
export REST_BASIC_AUTH_PASS=your_kafka_rest_basic_auth_pass
export CONNECT_BASIC_AUTH_USER=your_kafka_connect_basic_auth_user
export CONNECT_BASIC_AUTH_PASS=your_kafka_connect_basic_auth_pass
```

2. Ensure your Kafka topics, ACLs, and connectors are defined in JSON files within the `application1` directory.

## Usage

### Running KafkaManager

Execute the `main.py` script to apply changes based on git diffs:

```bash
python main.py
