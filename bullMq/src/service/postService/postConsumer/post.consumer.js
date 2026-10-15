import { Worker } from "bullmq"
//import { QueueEvents } from 'bullmq';
import { postDLQ } from "../../../broker/redis.broker";
import { connection } from "../../../config/redis.config";

//const postQueueEvents = new QueueEvents('ProductQueue');


export const consumingThePostFromTheQueue = async () =>{
    
    const postConsumer = new Worker(
        'PostQueue',
        async (job)=>{
            // call the repository layer to add the post in it
        },
        {connection}
    )

    postConsumer.on('completed', async(job)=>{
        try{
            console.log(`post with job id ${job.jobId} has been sucessfully added into the databse`)

            // add a message asynchronously into the EmailQueue, and send the notification to the followers of the user regarding the new post
        }catch(error){
            throw error
        }
    })

    postConsumer.on('failed',async(job)=>{
        try{
            await postDLQ.add('FailedPost',job,{connection})
            // send an email to the admin that a post with the given jobId has been added to DLQ.
        }catch(error){
            throw error
        }
    })

    // Return value becomes the result of waitUntilFinished()
    return {
        success: true,
        message: "Post published successfully.",
        data: savedPost,
    };
} 








/*
export const handlePostEvents = async ()=>{

    postQueueEvents.on('completed', async (job) => {
        console.log(`post with job id ${job.jobId} has been sucessfully added into the databse`)
        // add a notification asynchronously into the EmailQueue, and send the notification to the followers of the user regarding the new post 
    })

    postQueueEvents.on('failed', async(job)=>{
        try{
            await postDLQ.add('FailedPost',job,{connection})
            // send an email to the admin that a post with the given jobId has been added to DLQ.
        }catch(error){
            throw error
        }
    })
}

//

*/
