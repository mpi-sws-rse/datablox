#!/bin/bash

if [[ "$GENFORMA_PLATFORM" == "" ]]; then
    if [[ `uname` == "Darwin" ]]; then
        if [[ `uname -r` == "9.8.0" ]]; then
            echo macosx
        else
            echo macosx64
        fi
    else
      if [[ `uname` == "Linux" ]]; then
	  if [[ `uname -m` == "x86_64" ]]; then
	      echo "linux64"
	  else
	      echo "linux"
          fi
      else
	echo "Unknown platform!"
	exit 1
      fi
    fi
else
    echo $GENFORMA_PLATFORM
fi
