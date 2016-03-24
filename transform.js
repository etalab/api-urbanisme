const JSONStream = require('JSONStream');
const map = require("through2-map")
const fs = require('fs');
const request = require('request');

const parser = JSONStream.parse('features.*');

const src = request({
    url: 'https://inspire.data.gouv.fr/api/geogw/services/55673a34330f1fcd4832db30/feature-types/monuments_historiques_perimetr/download',
    qs: { format: 'GeoJSON', projection: 'WGS84' }
});

const dest = fs.createWriteStream(__dirname + '/data/servitudes_ac1.json');

src
    .pipe(parser)
    .pipe(map.obj(servitudeGeoJSON => ({
        nom: servitudeGeoJSON.properties.libelle,
        type: 'AC1',
        codeMerimee: servitudeGeoJSON.properties.cd_merimee,
        libelleCommune: servitudeGeoJSON.properties.lb_com,
        id: servitudeGeoJSON.properties.id_obj,
        assiette: servitudeGeoJSON.geometry
    })))
    .pipe(JSONStream.stringify())
    .pipe(dest);
