#!/bin/sh

input="$1"
output="$2"

ogr2ogr -f GeoJSON "$output" "$input"
