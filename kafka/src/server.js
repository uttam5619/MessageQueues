import express from "express"
import {config} from "dotenv"
import { appRouter } from "./routes/app.routes.js"
import { startServer } from "./config/server.config.js"
config()

const app = express()

app.use(express.json())
app.use('/kafka',appRouter)

startServer().then(() => {
    app.listen(process.env.PORT || 3000, () => {
        console.log(`Server listening on port ${process.env.PORT || 3000}`)
    })
})