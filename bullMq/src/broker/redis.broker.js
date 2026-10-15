
import {Queue} from "bullmq"
import connection from '../config/redis.config'

export const postQueue = Queue('PostQueue',{connection})
export const emailQueue = Queue('EmailQueue',{connection})

export const postDLQ = Queue('PostDLQ',{connection})
export const emailDLQ = Queue('EmailDLQ',{connection}) 