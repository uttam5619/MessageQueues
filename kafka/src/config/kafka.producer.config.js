import { Admin, Kafka, logLevel, Producer } from 'kafkajs'

class kafkaProducerConfig {
    kafka
    admin
    producer
    brokers

    constructor(){
        this.brokers = process.env.KAFKA_BROKER_URL,
        this.kafka = new Kafka({
            clientId:'post-producer',
            brokers:[this.brokers],
            logLevel:logLevel.ERROR
        })
        this.admin = this.kafka.admin()
        this.producer = this.kafka.producer()
    }

    async connect(){
        try{
            await this.producer.connect()
            await this.admin.connect()
            console.log('producer and admin are connected with kafka')
        }catch(err){
            console.log(err)
        }
    }

    async createTopic(topic){
        try{
            await this.admin.createTopics({
                topics:[{topic, numPartitions:1}]
            })
            console.log('topic created', topic)
        }catch(err){
            console.log('Error creating topic',err)
        }
    }

    async sentToTopic(topic, message){
        try{
            await this.producer.send({
                topic,
                messages:[{value:message}]
            })
            console.log(`${message} added to the topic:${topic}`)
        }catch(err){
            console.log(err)
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

export const kafkaProducerService = new kafkaProducerConfig()
