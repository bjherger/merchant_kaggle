#!/usr/bin/env python
"""
coding=utf-8

Code template courtesy https://github.com/bjherger/Python-starter-repo

"""
import logging
import os
import pickle
import sqlite3

import pandas

from bin import lib

CON = sqlite3.connect("../data/output/merchant.db")
CUR = CON.cursor()


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

    # Read and convert to sql
    pandas.read_csv(lib.get_conf('historical_transactions_path')).to_sql('historical_transactions', CON,
                                                                         if_exists='replace')
    pandas.read_csv(lib.get_conf('merchants_path')).to_sql('merchants', CON, if_exists='replace')
    pandas.read_csv(lib.get_conf('new_merchant_transactions_path')).to_sql('new_merchant_transactions', CON,
                                                                           if_exists='replace')
    pandas.read_csv(lib.get_conf('sample_submission_path')).to_sql('sample_submission', CON, if_exists='replace')

    lib.archive_dataset_schemas('extract', locals(), globals())
    logging.info('End extract')


def transform():
    logging.info('Begin transform')

    numerical_aggs = ['MIN', 'MAX', 'AVG']

    # historical_transactions
    vars = ['authorized_flag', 'installments', 'purchase_amount']
    table_name = 'historical_transactions'
    groupby_var = 'merchant_id'

    historical_transactions_aggs = create_aggs(table_name, groupby_var, vars, numerical_aggs)

    # merchants
    vars = ['numerical_1', 'numerical_2', 'avg_sales_lag3', 'avg_purchases_lag3', 'active_months_lag3',
            'avg_sales_lag6', 'avg_purchases_lag6', 'active_months_lag6', 'avg_sales_lag12', 'avg_purchases_lag12',
            'active_months_lag12']
    table_name = 'merchants'
    groupby_var = 'merchant_id'

    merchant_aggs = create_aggs(table_name, groupby_var, vars, numerical_aggs)

    # new_merchant_transactions
    vars = ['authorized_flag', 'installments', 'purchase_amount']
    table_name = 'new_merchant_transactions'
    groupby_var = 'card_id'

    new_merchant_transactions_aggs = create_aggs(table_name, groupby_var, vars, numerical_aggs)

    return


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


def create_aggs(table_name, gropuby_var, vars, numerical_aggs):
    features = list()
    for var in vars:
        for numerical_agg in numerical_aggs:
            variable_select = f'{numerical_agg}({var}) AS {var}_{numerical_agg.lower()}'
            features.append(variable_select)

    feature_query = ', '.join(features)

    # Drop the table if it exists already
    CUR.execute(f'DROP TABLE IF EXISTS {table_name}_agg;')

    query = f"""

    CREATE TABLE {table_name}_agg AS (
        SELECT {gropuby_var}, {feature_query} 
            FROM {table_name}
            GROUP BY {gropuby_var}
        );"""

    logging.info(f'query: {query}')

    CUR.execute(query)


# Main section
if __name__ == '__main__':
    main()
