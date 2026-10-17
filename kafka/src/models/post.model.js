

import { Schema , model } from 'mongoose'

const postSchema = new Schema({
    title:{
        type:String, 
        maxlength: [200, 'Title cannot exceed 200 characters'],
        trim: true
    },
    description:{
        type:String,
        maxlength: [2000, 'Description cannot exceed 2000 characters'],
        trim: true
    }
})

const Post = model('Post', postSchema)
export default Post