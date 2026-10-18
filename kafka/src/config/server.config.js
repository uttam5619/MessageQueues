import { kafkaProducerService as kafka} from "./kafka.producer.config.js"
import { kafkaConsumer } from "./kafka.consumer.config.js"
import { connectMongo } from "./mongo.config.js"


export const startServer = async ()=>{
    try{
        
        await kafka.connect()
        await kafka.createTopic('post')
        await kafkaConsumer.connect()
        await connectMongo()
    }catch(err){
        console.log('Failed to start the server', err)
        process.exit(1)
    }
    
}