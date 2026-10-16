import {config} from 'dotenv'
import { connectMongo } from './config/mongodb.config.js'
config()

connectMongo()