const JSONStream = require('JSONStream');
const map = require('through2-map')
const request = require('request');
const _ = require('lodash');
const ServitudeWriter = require('./lib/ServitudeWriter');

const datasets = {
    generateur: {
        resourceId: 'services/55673a34330f1fcd4832db30/feature-types/monuments_historiques_immeuble',
        source: 'passerelle',
        key: 'codeMerimee',
        mapping: {
            nom: 'properties.libelle',
            codeMerimee: 'properties.cd_merimee',
            generateur: 'geometry',
        },
        set: {
            type: 'AC1',
        },
    },
    assiette: {
        resourceId: 'services/55673a34330f1fcd4832db30/feature-types/monuments_historiques_perimetr',
        source: 'passerelle',
        key: 'codeMerimee',
        mapping: {
            nom: 'properties.libelle',
            codeMerimee: 'properties.cd_merimee',
            libelleCommune: 'properties.lb_com',
            assiette: 'geometry',
        },
        set: {
            type: 'AC1',
        },
    },
};

function getPasserelleRequest(resourceId) {
    return request({
        url: `https://inspire.data.gouv.fr/api/geogw/${resourceId}/download`,
        qs: { format: 'GeoJSON', projection: 'WGS84' }
    });
}

function getParser() {
    return JSONStream.parse('features.*');
}

function getServitudeWriter(key) {
    return new ServitudeWriter(key);
}

function importDataset(dataset, done) {
    getPasserelleRequest(dataset.resourceId)
        .pipe(getParser())
        .pipe(map.obj(row => {
            const transformedRow = {};
            _.forEach(dataset.mapping, (mappingDef, attrName) => {
                const val = _.get(row, mappingDef);
                if (val) transformedRow[attrName] = val;
            });
            _.forEach(dataset.set, (val, attrName) => {
                transformedRow[attrName] = val;
            });
            return transformedRow;
        }))
        .pipe(getServitudeWriter(dataset.key))
        .on('finish', () => {
            done();
        });
}

importDataset(datasets.assiette, err => {
    importDataset(datasets.generateur, err => {
        console.log('Import terminé');
        process.exit(0);
    });
});
