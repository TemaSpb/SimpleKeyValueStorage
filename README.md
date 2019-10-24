# SimpleKeyValueStorage

Simple key-value storage.

To start the server:
~~~
go run server.go priorityQueue.go
~~~

To start the client:
~~~
go run client.go
~~~

List of commands:
* PUT    <key\> <value\> <lifetime (seconds)>
* READ   <key\>
* DELETE <key\>
* STOP

Last command for stop the client. In case of putting new value if such key already exists and not expired yet, value will be updated.

TTL logic for keys realized via min heap (priority queue).