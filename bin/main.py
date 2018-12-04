#!/usr/bin/env python
"""
coding=utf-8

Code template courtesy https://github.com/bjherger/Python-starter-repo

"""
import logging
import os
import pickle

import pandas

from bin import lib


def main():
    """
    Main function documentation template
    :return: None
    :rtype: None
    """
    logging.basicConfig(level=logging.DEBUG)
    if lib.get_conf('perform_extract'):
        extract()
    if lib.get_conf('perform_transform'):
        transform()
    pass


def extract():
    logging.info('Begin extract')

    # Read
    historical_transactions = pandas.read_csv(lib.get_conf('historical_transactions_path'))
    merchants = pandas.read_csv(lib.get_conf('merchants_path'))
    new_merchant_transactions = pandas.read_csv(lib.get_conf('new_merchant_transactions_path'))
    sample_submission = pandas.read_csv(lib.get_conf('sample_submission_path'))
    test = pandas.read_csv(lib.get_conf('test_path'))
    train = pandas.read_csv(lib.get_conf('train_path'))

    # Convert to pickle
    historical_transactions.to_hdf(os.path.join(lib.get_conf('pickle_path'), 'historical_transactions.pkl'))
    merchants.to_hdf(os.path.join(lib.get_conf('pickle_path'), 'merchants.pkl'))
    new_merchant_transactions.to_hdf(os.path.join(lib.get_conf('pickle_path'), 'new_merchant_transactions.pkl'))
    sample_submission.to_hdf(os.path.join(lib.get_conf('pickle_path'), 'sample_submission.pkl'))

    lib.archive_dataset_schemas('extract', locals(), globals())
    logging.info('End extract')
    observations = None
    return observations


def transform():
    logging.info('Begin transform')
    data_sets = [('historical_transactions',
                  pandas.read_hdf(os.path.join(lib.get_conf('pickle_path'), 'historical_transactions.pkl'))),
                 ('merchants', pandas.read_hdf(os.path.join(lib.get_conf('pickle_path'), 'merchants.pkl'))),
                 ('new_merchant_transactions',
                  pandas.read_hdf(os.path.join(lib.get_conf('pickle_path'), 'new_merchant_transactions.pkl'))),
                 ('sample_submission',
                  pandas.read_hdf(os.path.join(lib.get_conf('pickle_path'), 'sample_submission.pkl')))]

    datatype_aggs = {'numerical': {'mean', 'min', 'max', 'median', 'std'},
                     'categorical': {'mode'}}

    for data_set_name, data_set in data_sets:
        for datatype_name, datatype_aggs in datatype_aggs.items():
            variables = datatype_aggs.union(data_set)
            data_set[variables].groupby()

    lib.archive_dataset_schemas('transform', locals(), globals())
    logging.info('End transform')
    return observations


def model(observations):
    logging.info('Begin model')

    mapper = None

    transformation_pipeline = None

    trained_model = None

    lib.archive_dataset_schemas('model', locals(), globals())
    logging.info('End model')
    return observations, transformation_pipeline, trained_model


def load(observations, transformation_pipeline, trained_model):
    logging.info('Begin load')

    # Reference variables
    lib.get_temp_dir()

    observations_path = os.path.join(lib.get_temp_dir(), 'observations.csv')
    logging.info('Saving observations to path: {}'.format(observations_path))
    observations.to_csv(observations_path, index=False)

    if transformation_pipeline is not None:
        transformation_pipeline_path = os.path.join(lib.get_temp_dir(), 'transformation_pipeline.pkl')
        logging.info('Saving transformation_pipeline to path: {}'.format(transformation_pipeline))
        pickle.dump(transformation_pipeline, open(transformation_pipeline, 'w+'))

    if trained_model is not None:
        trained_model_path = os.path.join(lib.get_temp_dir(), 'trained_model.pkl')
        logging.info('Saving trained_model to path: {}'.format(transformation_pipeline))
        pickle.dump(trained_model, open(trained_model_path, 'w+'))

    lib.archive_dataset_schemas('load', locals(), globals())
    logging.info('End load')
    pass


# Main section
if __name__ == '__main__':
    main()
