Simple no-deps full RPC code
============================

How to make archive
-------------------

* Build and start docker container with Dockerfile
* Edit dev_build_redeploy.sh - disable redeploy step at the end
* Run it

    bash dev_build_redeploy.sh DOCKER_CONTAINER_ID

* Archive would appears in arch/agent.sh


How to install agent on the cluster
-----------------------------------

* Create inventory file
* Password less ssh, all this
* Install agent locally on cluster master node

    bash arch/agent.sh /opt/mirantis/agent

* Install agent on the cluster

    bash /opt/mirantis/agent/deploy.sh deploy inventory

* Check agents statuses

    bash /opt/mirantis/agent/deploy.sh status --certs-folder /opt/mirantis/agent/agent_client_keys"  inventory


How to remove them
------------------

    bash /opts/mirantis/agent/deploy.sh remove inventory
