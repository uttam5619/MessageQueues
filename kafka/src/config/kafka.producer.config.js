import { Kafka, logLevel } from 'kafkajs'
import {config} from "dotenv"
config()
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
        await this.producer.connect()
        await this.admin.connect()
        console.log('producer and admin are connected with kafka')
    }

    async createTopic(topic){
        await this.admin.createTopics({
            topics:[{topic, numPartitions:1}]
        })
        console.log('topic created', topic)
    }

    async sentToTopic(topic, message){
        return this.producer.send({
            topic,
            messages:[{value:message}]
        })
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
