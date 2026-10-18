import express from "express"
import { createPost } from "../services/post/post.service.js"

export const appRouter = express.Router()
appRouter.post('/post', createPost)