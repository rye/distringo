#!/bin/sh

collection="$1"
year="$2"

wget -m "ftp://ftp2.census.gov/geo/tiger/TIGER${year}/${collection}/${year}/"
