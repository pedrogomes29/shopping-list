# shopping-list

## Running the Load Balancer
Replace 8080 with the wanted port and cont1.txt with the conf file path
```
gradlew LoadBalancer -Pport=8080 -PfilePath="conf1.txt"
```

## Running a node
Replace 100 with the wanted port and cont.txt with the conf file path
```
gradlew Node -Pport=100 -PfilePath="conf.txt"
```

## Runing the Client
```
gradlew Client --console=plain
```