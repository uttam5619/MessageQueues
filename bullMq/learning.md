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

## Terminologies

- Producer -> One who creates a job or message and then parks it into the message broker.

- Worker / Consumer -> One who fetches the job(s)/message(s) from the broker and process it.
    A running process that executes background jobs. It can process jobs without blocking the API request.

- (Queue / Message) Broker -> One who provides the infrastructure for the Queues.
    A broker is a middleware that receives, routes, and often stores messages between services.

- Consumer Group -> A group of consumers that cooperate to process a topic's partitions in kafka. It enables parallel processing and load distribution.

- Retry -> It is nothing but attempting a failed job or message again, often with a delay or exponential backoff.

- Dead-Letter Queue (DLQ) -> It stores messages that repeatedly fail processing, so they can be inspected and handled separately.

- Acknowledgment (ACK) -> Indicates that a message has been processed or handled according to the messaging system's rules.


## Working Mechanism 

# Setting up of the Queue using the broker.
```
import { Queue } from 'bullmq'
const queue = new Queue('queueName')
```

# Setting up of a producer
```
const job = {
    id:'101',
    message:'this is a test job'
}
queue.add('jobName', job)
```

# Setting up the consumer
```
import { Worker } from 'bullmq';

const worker = new Worker('queueName', async job => {
  if (job.name === 'jobName') {
    await console.log(job)
    await console.log(job.name)
    await console.log(job.data)
  }
});
```

For the consumer shown above, js will console following mentiond below.
```

Job {
  name: 'jobName',
  data: {
    id: '101',
    message: 'this is a test job'
  },
  
}

jobName

{ id: '101', message: 'this is a test job' }
```

Ex-
# Setting up an EMAIL queue 
```
import { Queue } from 'bullmq'
const emailQueue = new Queue('EMAIL')
```

# Setting up an email producer
```
const email ={
    jobId : 1001,
    jobContent : {
        from : 'uttam111@gmail.com',
        to : 'uttam123@gmail.com',
        content : 'this is a test email'
    }
}
emailQueue.add('emailService',email)
```

# Setting up an email consumer

```
import { Worker } from "bullmq"

const emailWorker = new Worker('EMAIL', (job)=>{
    if(job.name =="emailService"){
        await console.log(job.name)
        await console.log(job.data.id)
        await console.log(job.data.content.from)
        await console.log(job.data.content.to)
        await console.log(job.data.content.content)
    }
})
```