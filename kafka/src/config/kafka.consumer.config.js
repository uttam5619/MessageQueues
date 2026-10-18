import { Kafka, logLevel} from 'kafkajs'
import {config} from 'dotenv'
config()

class kafkaConsumerConfig {
    kafka
    admin
    consumer
    brokers

    constructor(){
        this.brokers = process.env.KAFKA_BROKER_URL,
        this.kafka = new Kafka({
            clientId:'post-producer',
            brokers:[this.brokers],
            logLevel:logLevel.ERROR
        })
        this.admin = this.kafka.admin()
        this.consumer = this.kafka.consumer({
            groupId:'post-consumer'
        })
    }

    async connect(){
        await this.consumer.connect()
        console.log('Consumer is connected with kafka')
    }

    async subscribeToTopic(topic){
        await this.consumer.subscribe({
            topic,
            fromBeginning:true
        })
    }

    async consumeMessage(callback) {
        await this.consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const value = message.value?.toString();
                const parsedMessage = JSON.parse(value);

                console.log("Message received", {
                    topic,
                    partition,
                    value: parsedMessage
                });

                await callback(parsedMessage);
            }
        });
    }


    async disconnect(){
        try{
            await this.consumer.disconnect()
            await this.admin.disconnect()
            console.log('kafka disconnected')
        }catch(err){
            console.log('Error occured while disconnecting with kafka', err)
        }
    }
}


export const kafkaConsumer = new kafkaConsumerConfig()
