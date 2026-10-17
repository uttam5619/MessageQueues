
import mongoose from 'mongoose'

export const connectMongo = async ()=>{
    try{
        await mongoose.connect(process.env.MONGODB_CONNECTION_URL)
        console.log('application server gets sucessfully connected with mongodb')
    }catch(error){
        console.log('failed to connect with mongodb')
    }
}