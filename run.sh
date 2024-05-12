#!/bin/bash
# Script for measuring the performance of the data2neo pipeline

# repeat the experiment 5 times
for i in {1..5}
do
    python import_csv.py --name csv_ansible_$i --database /myhome/benchmark_data/ansible__ansible__2020-06-11_06-06-22.db
    python import_data2neo.py --name data2neo_ansible_$i --database /myhome/benchmark_data/ansible__ansible__2020-06-11_06-06-22.db --workers 32
done

for i in {1..5}
do
    python import_csv.py --name csv_openshift_$i --database /myhome/benchmark_data/openshift__origin__2020-06-12_19-40-32.db
    python import_data2neo.py --name data2neo_openshift_$i --database /myhome/benchmark_data/openshift__origin__2020-06-12_19-40-32.db --workers 32

for i in {1..5}
do
    python import_csv.py --name csv_tensorflow_$i --database /myhome/benchmark_data/tensorflow__tensorflow__2020-06-13_06-54-28.db
    python import_data2neo.py --name data2neo_tensorflow_$i --database /myhome/benchmark_data/tensorflow__tensorflow__2020-06-13_06-54-28.db --workers 32
done