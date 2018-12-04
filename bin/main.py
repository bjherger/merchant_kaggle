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
    logging.getLogger().setLevel(logging.DEBUG)
    if lib.get_conf('perform_extract'):
        extract()
    if lib.get_conf('perform_transform'):
        transform()
    if lib.get_conf('perform_train'):
        train()
    pass


def extract():
    logging.info('Begin extract')

    # Read and convert to sql
    pandas.read_csv(lib.get_conf('historical_transactions_path')).to_sql('historical_transactions', CON,
                                                                         if_exists='replace')
    pandas.read_csv(lib.get_conf('merchants_path')).to_sql('merchants', CON, if_exists='replace')
    pandas.read_csv(lib.get_conf('new_merchant_transactions_path')).to_sql('new_merchant_transactions', CON,
                                                                           if_exists='replace')
    pandas.read_csv(lib.get_conf('train_path')).to_sql('train', CON, if_exists='replace')
    pandas.read_csv(lib.get_conf('test_path')).to_sql('test', CON, if_exists='replace')

    lib.archive_dataset_schemas('extract', locals(), globals())
    logging.info('End extract')


def transform():
    logging.info('Begin transform')

    numerical_aggs = ['MIN', 'MAX', 'AVG']

    # historical_transactions
    vars = ['authorized_flag', 'installments', 'purchase_amount']
    table_name = 'historical_transactions'
    groupby_var = 'merchant_id'

    create_aggs(table_name, groupby_var, vars, numerical_aggs)

    # merchants
    vars = ['numerical_1', 'numerical_2', 'avg_sales_lag3', 'avg_purchases_lag3', 'active_months_lag3',
            'avg_sales_lag6', 'avg_purchases_lag6', 'active_months_lag6', 'avg_sales_lag12', 'avg_purchases_lag12',
            'active_months_lag12']
    table_name = 'merchants'
    groupby_var = 'merchant_id'

    create_aggs(table_name, groupby_var, vars, numerical_aggs)

    # new_merchant_transactions
    vars = ['authorized_flag', 'installments', 'purchase_amount']
    table_name = 'new_merchant_transactions'
    groupby_var = 'card_id'

    create_aggs(table_name, groupby_var, vars, numerical_aggs)

    # Create merchant_id_lookup
    query = f"""
    DROP TABLE IF EXISTS merchant_id_lookup;
    CREATE TABLE merchant_id_lookup AS
        SELECT * 
            FROM merchants_agg
                LEFT JOIN new_merchant_transactions_agg ON merchant_id;
    """
    CUR.executescript(query)

    # Create card_id_lookup
    query = f"""
        DROP TABLE IF EXISTS card_id_lookup;
        CREATE TABLE card_id_lookup AS
            SELECT * 
                FROM new_merchant_transactions_agg;
        """
    CUR.executescript(query)

    logging.info('End transform')


def train():
    logging.info('Begin train')

    observation_query = """
    SELECT * 
        FROM train
            LEFT JOIN merchant_id_lookup ON merchant_id
            LEFT JOIN card_id_lookup ON card_id;
    """

    observations = pandas.read_sql(observation_query, CON)

    lib.archive_dataset_schemas('train', locals(), globals())
    logging.info('End train')


def create_aggs(table_name, gropuby_var, vars, numerical_aggs):
    features = list()
    for var in vars:
        for numerical_agg in numerical_aggs:
            variable_select = f'{numerical_agg}({var}) AS {var}_{numerical_agg.lower()}'
            features.append(variable_select)

    feature_query = ', '.join(features)

    # Drop the table if it exists already
    CUR.execute(f'DROP TABLE IF EXISTS {table_name}_agg;')

    # Create aggregate table
    query = f"""
    CREATE TABLE {table_name}_agg AS 
        SELECT {gropuby_var}, {feature_query} 
            FROM {table_name}
            GROUP BY {gropuby_var}
        ;"""

    logging.info(f'query: {query}')

    CUR.execute(query)


# Main section
if __name__ == '__main__':
    main()
