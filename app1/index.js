const express = require("express")
const kafka = require("kafka-node")
const sequelize = require("sequelize")
const app = express()
app.use(express.json())


const fine = async () => {
    
    const db = new sequelize(process.env.POSTGRES_URL)
    const user = db.define("user", {
        name: sequelize.STRING,
        email: sequelize.STRING,
        password: sequelize.STRING
    })
    db.sync({ force: true })
    const client = new kafka.KafkaClient({ kafkaHost: process.env.KAFKA_BOOSTRAP_URL })
    const producer = new kafka.Producer(client)
    producer.on('ready', async () => {
        console.log(">>>>>>>>>>>>>>>>>")
        app.post("/", async (req, res) => {
            console.log("><<<<<<<<<<<<<",req.body)
            producer.send([{
                topic: process.env.KAFKA_Topic, messages: JSON.stringify(req.body)
            }], (err, data) => {
                if (err) console.log(err);
                else user.create(req.body)
            })
        })
    })
}

setTimeout(fine, 1000)

app.listen(process.env.PORT,()=>{
    console.log("listening to ",process.env.PORT)
})