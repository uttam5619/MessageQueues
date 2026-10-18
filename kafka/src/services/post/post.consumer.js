import { kafkaConsumer } from "../../config/kafka.consumer.config.js";
import Post from "../../models/post.model.js";

const BATCH_SIZE = 5000;
const FLUSH_INTERVAL = 4000;

const messageList = [];
let processing = false;
let consumerStarted = false;
let flushTimer;

const scheduleFlush = () => {
    if (flushTimer || messageList.length === 0) return;

    flushTimer = setTimeout(async () => {
        flushTimer = null;
        await processMessages();
    }, FLUSH_INTERVAL);
};

/*
const processMessages = async () => {
    if (processing || messageList.length === 0) return;

    processing = true;
    const batch = messageList.splice(0, BATCH_SIZE);

    try {
        await Post.insertMany(batch, { ordered: false });
        console.log("Bulk insertion completed");
    } catch (error) {
        messageList.unshift(...batch);
        console.error("Failed to bulk insert posts:", error);
    } finally {
        processing = false;
    }
};
*/

const processMessages = async () => {
    if (processing) return;

    processing = true;

    try {
        while (messageList.length > 0) {
            const batch = messageList.splice(0, BATCH_SIZE);

            try {
                await Post.insertMany(batch, { ordered: false });
                console.log(`Persisted ${batch.length} posts`);
            } catch (error) {
                // Do not blindly retry the entire batch:
                // some documents may already have been inserted.
                console.error("Batch persistence failed:", error);
                messageList.unshift(...batch);
                break;
            }
        }
    } finally {
        processing = false;

        if (messageList.length > 0) {
            scheduleFlush();
        }
    }
};

export const postConsumer = async () => {
    if (consumerStarted) return;

    consumerStarted = true;
    try {
        await kafkaConsumer.subscribeToTopic("post");
        await kafkaConsumer.consumeMessage(async (message) => {
            messageList.push(message);
            if (messageList.length >= BATCH_SIZE) {
                await processMessages();
            } else {
                scheduleFlush();
            }
        });
    } catch (error) {
        consumerStarted = false;
        throw error;
    }
};
