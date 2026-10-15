### Learning

bullmq -> It is a Redis-based distributed queue for Node.js, Python, Elixir, .NET, Rust, PHP, and more.

To integrate bullmq with the Node/Bun run time we have thee command
```
npm install bullmq
```
Similarly for python we have 

```
pip install bullmq
```

- Producer -> One who creates a job or message and then parks it into the message broker.
- Worker / Consumer -> One who fetches the job(s)/message(s) from the broker and process it.
- Queue / Message Broker -> One who provides the infrastructure for the Queues