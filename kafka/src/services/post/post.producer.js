import { kafkaProducerService } from "../../config/kafka.producer.config.js";

export const produceMessage = (post) =>
    kafkaProducerService.sentToTopic("post", JSON.stringify(post));
