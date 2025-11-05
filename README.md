# Distributed Systems Labs

## Setup

```bash
docker compose up --build
```
The frontend should be available under `http://localhost:8000/`

## Example Output
```
app-1  | #### Starting labs server with 10 nodes
app-1  | serving on 0.0.0.0:80 view at http://127.0.0.1:80
app-1  | Sending message from 0 to 1
app-1  | Sending message from 0 to 2
app-1  | Sending message from 0 to 3
app-1  | Sending message from 0 to 4
app-1  | Sending message from 0 to 5
app-1  | Sending message from 0 to 6
app-1  | Sending message from 0 to 7
app-1  | Sending message from 0 to 8
app-1  | Sending message from 0 to 9
app-1  | Delivering message at time 1.9500000000000015: "Hello from 0"
app-1  | Delivering message at time 1.9500000000000015: "Hello from 0"
app-1  | Delivering message at time 1.9500000000000015: "Hello from 0"
app-1  | Delivering message at time 1.9500000000000015: "Hello from 0"
app-1  | Delivering message at time 1.9500000000000015: "Hello from 0"
app-1  | Delivering message at time 1.9500000000000015: "Hello from 0"
app-1  | Delivering message at time 1.9500000000000015: "Hello from 0"
app-1  | Delivering message at time 1.9500000000000015: "Hello from 0"
app-1  | Delivering message at time 1.9500000000000015: "Hello from 0"
app-1  | Messenger 9 received message
app-1  | Node 9 received message: "Hello from 0"
app-1  | Messenger 7 received message
app-1  | Node 7 received message: "Hello from 0"
app-1  | Messenger 6 received message
app-1  | Node 6 received message: "Hello from 0"
app-1  | Messenger 8 received message
app-1  | Node 8 received message: "Hello from 0"
app-1  | Messenger 2 received message
app-1  | Node 2 received message: "Hello from 0"
app-1  | Messenger 5 received message
app-1  | Node 5 received message: "Hello from 0"
app-1  | Messenger 1 received message
app-1  | Node 1 received message: "Hello from 0"
app-1  | Messenger 4 received message
app-1  | Node 4 received message: "Hello from 0"
app-1  | Messenger 3 received message
app-1  | Node 3 received message: "Hello from 0"
app-1  | Sending message from 2 to 0
app-1  | Sending message from 2 to 1
app-1  | Sending message from 2 to 3
app-1  | Sending message from 2 to 4
app-1  | Sending message from 2 to 5
app-1  | Sending message from 2 to 6
app-1  | Sending message from 2 to 7
app-1  | Sending message from 2 to 8
app-1  | Sending message from 2 to 9
app-1  | Delivering message at time 9.13999999999985: "Hello from 2"
app-1  | Delivering message at time 9.13999999999985: "Hello from 2"
app-1  | Delivering message at time 9.13999999999985: "Hello from 2"
app-1  | Delivering message at time 9.13999999999985: "Hello from 2"
app-1  | Delivering message at time 9.13999999999985: "Hello from 2"
app-1  | Delivering message at time 9.13999999999985: "Hello from 2"
app-1  | Delivering message at time 9.13999999999985: "Hello from 2"
app-1  | Delivering message at time 9.13999999999985: "Hello from 2"
app-1  | Delivering message at time 9.13999999999985: "Hello from 2"
app-1  | Messenger 1 received message
app-1  | Node 1 received message: "Hello from 2"
app-1  | Messenger 9 received message
app-1  | Node 9 received message: "Hello from 2"
app-1  | Messenger 0 received message
app-1  | Node 0 received message: "Hello from 2"
app-1  | Messenger 8 received message
app-1  | Node 8 received message: "Hello from 2"
app-1  | Messenger 5 received message
app-1  | Node 5 received message: "Hello from 2"
app-1  | Messenger 4 received message
app-1  | Node 4 received message: "Hello from 2"
app-1  | Messenger 7 received message
app-1  | Node 7 received message: "Hello from 2"
app-1  | Messenger 3 received message
app-1  | Node 3 received message: "Hello from 2"
app-1  | Messenger 6 received message
app-1  | Node 6 received message: "Hello from 2"
```

