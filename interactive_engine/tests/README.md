### how to test
* ssh to remote host
```
ssh root@ip
```
* enter into work directory
```
cd GraphScope/interactive_engine/tests
```
* start service (vineyard server / coordinator / manager server ...)
```
./function_test.sh _start <client-server port> <vineyard-server num> 1>log 2>&1 
```
* start test ( load modern graph -> create interactive engine instance -> run gremlin test )
```
# see reports under target/surefire-reports/testng-results.xml
./function_test.sh _test 
```
* stop service
```
./function_test.sh _stop
```
### for debug
* remove gremlin test procedure from testng to execute queries in console
```
sed -i -e "s/junit=.*/junit=\"false\">/g" ./testng.xml
./function_test.sh _test
```
* recover gremlin test 
```
sed -i -e "s/junit=.*/junit=\"true\">/g" ./testng.xml
./function_test.sh _test
```