import { postQueue } from '../../../broker/redis.broker.js'

const addPostToPostQueue = async (post)=>{

    try{
        await postQueue.add(
            'publishUserPost',
            post,
            {
                attempts: 3,
                backoff: {
                    type: "exponential",
                    delay: 1000,
                },
            }
        )
        return await job.waitUntilFinished(postQueueEvents, 12000); 
    }catch(error){
        return {
            success: false,
            message: "Post publishing failed. We will retry processing it later.",
        };
    }
    
}