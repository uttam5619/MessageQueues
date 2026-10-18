
import mongoose from 'mongoose'

export const connectMongo = async ()=>{
    try{
        const connectionUrl = process.env.MONGODB_CONNECTION_URL
        if (!connectionUrl) {
            throw new Error('MONGODB_CONNECTION_URL is not configured')
        }

        await mongoose.connect(connectionUrl)
        console.log('Application server connected to MongoDB')
    }catch(error){
        console.error('Failed to connect to MongoDB:', error)
        throw error
    }
}
