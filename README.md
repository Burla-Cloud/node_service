### The repository for this service has been moved into the [Burla](https://github.com/Burla-Cloud/burla) GitHub repo.

It can be found in the `node_service` folder in the top-level directory.  
This was done only to reduce cycle time. Making one github release is faster and simpler than making 4.

#### Node Service

This is a component of the open-source cluster compute software [Burla](https://github.com/Burla-Cloud/burla).

The "node service" is a fastapi webservice that runs inside each Node (virtual machine) in the cluster.  
This service is responsible for starting/stopping/managing Docker containers running on the node.  
It also serves as a router responsible for delegating requests to the correct container.
