import { Types } from "mongoose";
import { QueueEvents } from "bullmq";

import { connection } from "../../config/redis.config.js";
import { postQueue } from "../../broker/redis.broker.js";

import {
    consumingThePostFromTheQueue,
} from "./postConsumer/post.consumer.js";

import {
    addPostToPostQueue,
} from "./postProducer/post.producer.js";

const QUEUE_NAME = "PostQueue";
const JOB_TIMEOUT = 30000;

const postQueueEvents = new QueueEvents(QUEUE_NAME, {
    connection,
});

let queueEventsReady;

const ensureQueueEventsReady = async () => {
    if (!queueEventsReady) {
        queueEventsReady = postQueueEvents.waitUntilReady().catch(
            (error) => {
                queueEventsReady = null;
                throw error;
            }
        );
    }

    await queueEventsReady;
};

export const createPost = async (postData) => {
    // 1. Generate a stable post ID before enqueueing.
    const post = {
        ...postData,
        _id: postData._id ?? new Types.ObjectId(),
    };

    // 2. Ensure the consumer is running.
    consumingThePostFromTheQueue();

    // 3. Ensure queue completion events are ready.
    await ensureQueueEventsReady();

    // 4. Send the post to BullMQ.
    const queuedResult = await addPostToPostQueue(post);

    // 5. Retrieve the queued job.
    const job = await postQueue.getJob(queuedResult.jobId);

    if (!job) {
        throw new Error(
            `Queued post job ${queuedResult.jobId} could not be found`
        );
    }

    // 6. Wait until the consumer persists the post
    //    and BullMQ marks the job completed.
    const result = await job.waitUntilFinished(
        postQueueEvents,
        JOB_TIMEOUT
    );

    // 7. Return the persistence result.
    return {
        ...result,
        jobId: queuedResult.jobId,
        postId: post._id.toString(),
    };
};
