# shopping-list

The DNS Mock needs to be running when adding load balancers so that it stores the IP/Port for the new load balancers.

Load Balancers and Nodes are added dynamically, providing a list of a few (at least 1) neighbor endpoints so that it can be added to the network topology.

The cloud needs at least 3 nodes to work properly.

Configuration files should contain a list of IP:port of the nodes that are already in network and to which the newly added node should connect to.

## Video Demonstration
https://www.youtube.com/watch?v=Ox6AhVOMrng

## Runing the DNS Mock
```
gradlew DnsMock
```

## Running a Load Balancer
Replace 8080 with the wanted port and conf1.txt with the conf file
```
gradlew LoadBalancer -Pid="lb1" -Pport=8080 -Pconf="conf.txt"
```

## Running an Admin
Replace 7070 with the wanted port and conf1.txt with the conf file
```
gradlew Admin -Pid="admin" -Pport=7070 -Pconf"conf.txt"
```

## Running a Node
Replace 100 with the wanted port and conf.txt with the conf file
```
gradlew Node -Pid="node1" -Pport=100 -Pconf="conf.txt"
```

## Runing the Client
```
gradlew Client --console=plain
```
