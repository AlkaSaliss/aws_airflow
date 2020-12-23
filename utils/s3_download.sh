#!/bin/bash


while [ $# -gt 0 ]; do
  case "$1" in
    -b=*)
      bucket="${1#*=}"
      ;;
    -f=*)
      folder="${1#*=}"
      ;;
	-o=*)
	  out="${1#*=}"
	  ;;
    *)
      printf "***************************\n"
      printf "* Error: Invalid argument.*\n"
      printf "***************************\n"
      exit 1
  esac
  shift
done

aws s3 cp --recursive "s3://${bucket}/${folder}" ${out}
