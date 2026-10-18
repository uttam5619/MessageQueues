import { produceMessage } from "./post.producer.js";
import { postConsumer } from "./post.consumer.js";

export const createPost = async (req, res) => {
    try {
        const { title, description } = req.body;
        if (!title || !description) {
            return res.status(400).json({
                success: false,
                message: "title and description are mandatory",
            });
        }

        await postConsumer();
        await produceMessage({ title, description });

        return res.status(202).json({
            success: true,
            message: "Post accepted for processing",
        });
    } catch (err) {
        return res.status(500).json({
            success: false,
            message: "Failed to process post",
        });
    }
};

export default createPost;