# shopping-list

Load Balancers and Nodes are added dynamically, providing a list of a few (at least 1) neighbor endpoints so that it can be added to the network topology

The DNS Mock needs to be running when adding load balancers so that it stores the IP/Port for the new load balancers


## Running a Load Balancer
Replace 8080 with the wanted port and conf1.txt with the conf file
```
gradlew LoadBalancer -Pport=8080 -PfilePath="conf.txt"
```

## Running an Admin
Replace 7070 with the wanted port and conf1.txt with the conf file
```
gradlew Admin -Pport=7070 -PfilePath="conf.txt"
```

## Running a Node
Replace 100 with the wanted port and conf.txt with the conf file
```
gradlew Node -Pport=100 -PfilePath="conf.txt"
```

## Runing the DNS Mock
```
gradlew DnsMock
```

## Runing the Client
```
gradlew Client --console=plain
```
