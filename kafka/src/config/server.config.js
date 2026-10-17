import { kafkaProducerService as kafka} from "./kafka.producer.config"
import { connectMongo } from "./mongo.config"


const startServer = async ()=>{
    try{
        
        await kafka.connect()
        await kafka.createTopic('post')
        await connectMongo()
    }catch(err){
        console.log('Failed to start the server', err)
        process.exit(1)
    }
    
}