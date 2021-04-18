#!/bin/sh

year="$1"

case "$year" in

	"2010")
		wget -m "ftp://ftp2.census.gov/geo/tiger/TIGER2010/"
		;;

	"2020")
		wget -m "ftp://ftp2.census.gov/geo/tiger/TIGER2020PL/STATE/"
		;;

esac
