import { postQueue } from "../../../broker/redis.broker.js";

export const addPostToPostQueue = async (post) => {
    try {
        const job = await postQueue.add("publishUserPost", post, {
            attempts: 3,
            backoff: {
                type: "exponential",
                delay: 1000,
            },
        });

        return {
            success: true,
            message: "Post queued successfully",
            jobId: job.id,
        };
    } catch (error) {
        throw error;
    }
};