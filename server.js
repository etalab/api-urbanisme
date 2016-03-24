const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const MongoClient = require('mongodb').MongoClient;

function getCollection(done) {
    MongoClient.connect(process.env.MONGODB_URL || 'mongodb://localhost/urba', function (err, db) {
        if (err) return done(err);
        console.log('Connected to database');
        db.collection('servitudes', function (err, servColl) {
            if (err) return done(err);
            servColl.createIndex({ assiette: '2dsphere' }, function (err) {
                if (err) return done(err);
                done(null, servColl);
            });
        });
    });
}

getCollection(function (err, servColl) {
    if (err) {
        console.error(err);
        process.exit(1);
    }

    const app = express();

    app.use(cors());
    app.use(bodyParser.json());

    // Inject collection
    app.use(function (req, res, next) {
        req.servColl = servColl;
        next();
    });

    app.post('/servitudes', function (req, res, next) {
        servColl
            .find({ assiette: { $geoIntersects: { $geometry: req.body.geom } } })
            .project({ assiette: 0 })
            .toArray(function (err, servitudes) {
                if (err) return next(err);
                res.send(servitudes);
            });
    });

    const listenPort = process.env.PORT || 5000;

    app.listen(listenPort, function () {
        console.log('Start listening on port ' + listenPort);
    });
})
