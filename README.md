# shopping-list

Load Balancers and Nodes are added dynamically, providing a list of a few (at least 1) neighbor endpoints so that it can be added to the network topology

The DNS Mock needs to be running when adding load balancers so that it stores the IP/Port for the new load balancers

Note: 
1. The cloud needs at least 3 nodes to work properly. 
2. The DNS Mock needs to be running before adding load balancers


## Running a Load Balancer
Replace 8080 with the wanted port and conf1.txt with the conf file
```
gradlew LoadBalancer -Pid="lb1" -Pport=8080 -Pconf="conf.txt"
```

## Running an Admin
Replace 7070 with the wanted port and conf1.txt with the conf file
```
gradlew Admin  -Pport=7070 -Pconf"conf.txt"
```

## Running a Node
Replace 100 with the wanted port and conf.txt with the conf file
```
gradlew Node -Pid="node1" -Pport=100 -Pconf="conf.txt"
```

## Runing the DNS Mock
```
gradlew DnsMock
```

## Runing the Client
```
gradlew Client --console=plain
```
