import { Worker } from "bullmq"
//import { QueueEvents } from 'bullmq';
import { postDLQ } from "../../../broker/redis.broker.js";
import { connection } from "../../../config/redis.config.js";

import Post from "../../../models/post.model.js";

//const postQueueEvents = new QueueEvents('ProductQueue');

const QUEUE_NAME = "PostQueue";
const BATCH_SIZE = 50;
const FLUSH_INTERVAL = 2000;
const WORKER_CONCURRENCY = 100;

let worker;
let flushTimer = null;
let flushPromise = null;
let flushAllRequested = false;

const pendingJobs = [];

const scheduleFlush = () => {
    if (flushTimer || pendingJobs.length === 0) return;

    flushTimer = setTimeout(() => {
        flushTimer = null;
        void flushBatch(true);
    }, FLUSH_INTERVAL);
};

const persistBatch = async (batch) => {
    const operations = batch.map(({ job }) => ({
        updateOne: {
            filter: { _id: job.data._id },
            update: {
                $setOnInsert: job.data,
            },
            upsert: true,
        },
    }));

    try {
        await Post.bulkWrite(operations, { ordered: false });

        for (const entry of batch) {
            entry.resolve({
                success: true,
                postId: entry.job.data._id.toString(),
            });
        }
    } catch (error) {
        // MongoDB may have persisted some documents before
        // reporting individual write errors.
        const writeErrors =
            error.writeErrors ??
            error.result?.getWriteErrors?.() ??
            [];

        if (writeErrors.length > 0 && !error.writeConcernError) {
            const failedIndexes = new Set(
                writeErrors.map((writeError) => writeError.index)
            );

            batch.forEach((entry, index) => {
                if (failedIndexes.has(index)) {
                    entry.reject(error);
                } else {
                    entry.resolve({
                        success: true,
                        postId: entry.job.data._id.toString(),
                    });
                }
            });

            return;
        }

        // For network/uncertain outcomes, retry the entire
        // batch safely using the stable MongoDB document IDs.
        for (const entry of batch) {
            entry.reject(error);
        }
    }
};

const flushBatch = (flushAll = false) => {
    if (flushAll) {
        flushAllRequested = true;
    }

    if (flushPromise) {
        return flushPromise;
    }

    flushPromise = (async () => {
        while (
            pendingJobs.length >= BATCH_SIZE ||
            flushAllRequested
        ) {
            if (pendingJobs.length === 0) {
                flushAllRequested = false;
                break;
            }

            const count = Math.min(
                BATCH_SIZE,
                pendingJobs.length
            );

            const batch = pendingJobs.splice(0, count);

            await persistBatch(batch);

            if (
                flushAllRequested &&
                pendingJobs.length === 0
            ) {
                flushAllRequested = false;
            }
        }
    })().finally(() => {
        flushPromise = null;

        // Handle jobs that arrived while another flush was running.
        if (
            pendingJobs.length >= BATCH_SIZE ||
            flushAllRequested
        ) {
            void flushBatch();
        } else {
            scheduleFlush();
        }
    });

    return flushPromise;
};

export const consumingThePostFromTheQueue = () => {
    if (worker) return worker;

    worker = new Worker(
        QUEUE_NAME,
        async (job) => {
            // The processor remains active until its batch is persisted.
            return new Promise((resolve, reject) => {
                pendingJobs.push({
                    job,
                    resolve,
                    reject,
                });

                if (pendingJobs.length >= BATCH_SIZE) {
                    if (flushTimer) {
                        clearTimeout(flushTimer);
                        flushTimer = null;
                    }

                    void flushBatch();
                } else {
                    scheduleFlush();
                }
            });
        },
        {
            connection,
            concurrency: WORKER_CONCURRENCY,
        }
    );

    worker.on("completed", (job) => {
        console.log(
            `Post ${job.data._id} persisted successfully; job ${job.id} completed`
        );

        // Trigger notification jobs here, after persistence.
    });

    worker.on("failed", async (job, error) => {
        if (!job) return;

        console.error(
            `Job ${job.id} failed on attempt ${job.attemptsMade}:`,
            error.message
        );

        if (job.attemptsMade < (job.opts.attempts ?? 1)) {
            return;
        }

        try {
            await postDLQ.add(
                "FailedPost",
                {
                    originalJobId: job.id,
                    data: job.data,
                    failedReason: error.message,
                },
                {
                    // Avoid duplicate DLQ records for the same job.
                    jobId: `failed-post-${job.id}`,
                }
            );
        } catch (dlqError) {
            console.error(
                `Failed to enqueue job ${job.id} in DLQ:`,
                dlqError
            );
        }
    });

    worker.on("error", (error) => {
        console.error("Post worker error:", error);
    });

    return worker;
};

export const closePostWorker = async () => {
    if (!worker) return;

    // Stop accepting new work and wait for active processors.
    // Their pending batches are still flushed by the timer.
    await worker.close();

    if (flushTimer) {
        clearTimeout(flushTimer);
        flushTimer = null;
    }

    // Flush any remaining batch before exiting.
    await flushBatch(true);

    worker = null;
};