import { Admin, Kafka, logLevel, Consumer } from 'kafkajs'

class kafkaProducerConfig {
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
        try{
            await this.consumer.connect()
            console.log('Consumer is connected with kafka')
        }catch(err){
            console.log(err)
        }
    }

    async subscribeToTopic(topic){
        try{
            await this.consumer.subscribe({
                topic,
                fromBeginning:true
            })
        }catch(err){
            console.log('Error occured while subscribing the topic',err)
        }
    }

    async consumeMessage(callback) {
        try {
            await this.consumer.run({
                eachMessage: async ({ topic, partition, message }) => {
                    try {
                        const value = message.value?.toString();
                        const parsedMessage = JSON.parse(value);

                        console.log("Message received", {
                            topic,
                            partition,
                            value: parsedMessage
                        });

                        await callback(parsedMessage);
                    } catch (err) {
                        console.error("Error processing message:", err);
                        throw err;
                    }
                }
            });
        } catch (err) {
            console.error("Error starting Kafka consumer:", err)
        }
    }


    async disconnect(){
        try{
            await this.producer.disconnect()
            await this.admin.disconnect()
            console.log('kafka disconnected')
        }catch(err){
            console.log('Error occured while disconnecting with kafka', err)
        }
    }
}


export default kafkaProducerConfig