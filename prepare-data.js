'use strict';
const JSONStream = require('JSONStream');
const map = require('through2-map')
const request = require('request');
const _ = require('lodash');
const ServitudeWriter = require('./lib/ServitudeWriter');
const Promise = require('bluebird');
const getCollection = require('./lib/mongodb').getCollection;
const debug = require('debug')('prepare-data');
const turf = require('turf');

const datasets = [
    // Midi-Pyrénées
    {
        coverage: ['dep09', 'dep12', 'dep21', 'dep32', 'dep46', 'dep65', 'dep81', 'dep82'],
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
    {
        coverage: ['dep09', 'dep12', 'dep21', 'dep32', 'dep46', 'dep65', 'dep81', 'dep82'],
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
    // Rhône-Alpes
    {
        coverage: ['dep73'],
        resourceId: 'file-packages/61e138a0f0287e348941002cef37d138b9ca6ed9/N_MONUMENT_HISTO_S_073',
        source: 'passerelle',
        key: 'codeMerimee',
        filters: ['computeAssietteAC1'],
        mapping: {
            nom: 'properties.IMMEUBLE',
            codeMerimee: 'properties.REF_MERIM',
            libelleCommune: 'properties.COMMUNE',
            generateur: 'geometry',
        },
        set: {
            type: 'AC1',
        },
    },
];

const filters = {
        computeAssietteAC1: row => {
            if (row.generateur) {
                const feature = { type: 'Feature', geometry: row.generateur };
                const buffer = turf.merge(turf.buffer(feature, 500, 'meters')).geometry;
                row.assiette = buffer;
            }
            return row;
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
    debug('importing dataset');
    let count = 0;
    return new Promise((resolve, reject) => {
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
                if (dataset.filters) {
                    _.forEach(dataset.filters, filterName => filters[filterName](transformedRow));
                }
                count++;
                return transformedRow;
            }))
            .pipe(getServitudeWriter(dataset.key))
            .on('finish', () => {
                debug('finished: %d', count);
                resolve();
            })
            .on('error', reject);
    });
}

function removeData() {
    debug('removing data');
    return getCollection().then(servColl => servColl.remove({}));
}

function removeAssietteIndex() {
    debug('removing index');
    return getCollection().then(servColl => servColl.createIndex({ assiette: '2dsphere' }));
}

function createAssietteIndex() {
    debug('creating index');
    return getCollection().then(servColl => servColl.createIndex({ assiette: '2dsphere' }));
}

function importAllDatasets() {
    return Promise.each(datasets, importDataset);
}

removeAssietteIndex()
    .then(removeData)
    .then(importAllDatasets)
    .then(createAssietteIndex)
    .then(() => {
        console.log('Import terminé');
        process.exit(0);
    })
    .catch(err => {
        console.error(err);
        process.exit(1);
    });
