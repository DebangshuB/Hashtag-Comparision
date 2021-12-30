const express = require('express');
const path = require('path');
const mongoose = require('mongoose');

const app = express();

const mongoDB = 'mongodb://127.0.0.1/hashtag-stats';
mongoose.connect(mongoDB, { useNewUrlParser: true, useUnifiedTopology: true });

const db = mongoose.connection;
db.on('error', console.error.bind(console, 'MongoDB connection error:'));

const Schema = mongoose.Schema;

var hashtag_data_schema = new Schema({
    id: mongoose.ObjectId,
    hashtags: String,
    sentiment: mongoose.Decimal128,
    count: Number,
    window: {
        start: Date,
        end: Date
    }
}, { collection: 'tweets' });

var hashtag_data = mongoose.model('hashtag_data', hashtag_data_schema);


app.use(express.static(path.join(__dirname, "static")));

PORT = 3000

app.get('/', function(req, res) {
    res.sendFile(path.join(__dirname, "static", '/index.html'));
});


app.get('/api', function(req, res) {

    hashtag_data.find({
        "window.start": { $gte: Date.now() - 240000 }
    }, "hashtags sentiment count", function(err, data) {

        if (err) {
            res.sendStatus(400)
            return handleError(err);
        } else {
            res.json({ data: data })
            console.log({ data: data })
        }
    })


});

app.listen(PORT, (req, res) => {
    console.log("Server listening on port " + PORT)
});