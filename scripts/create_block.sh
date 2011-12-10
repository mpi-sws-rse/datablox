#!/bin/bash

# Creates the skeleton for a block as well as its engage driver.
# Takes the blockname as an argument. The blockname wil be converted to
# lower case. The directory name will be
# adjusted to have all lower case and underscores.


if [[ "$BLOXPATH" == "" ]]; then
  BLOXPATH=`pwd`/blox
fi
if ! [ -d $BLOXPATH ]; then
  echo "Unable to find blox directory at $BLOXPATH"
  exit 1
fi

ENGAGE=`cd $BLOXPATH/../engage; pwd`
if ! [ -d $ENGAGE ]; then
  echo "Unable to find engage directory at $ENGAGE"
  exit 1
fi


if [[ "$#" != "1" ]]; then
  echo "create_block.sh <bockname>"
  exit 1
fi

# The block module name name should be all lower case and a valid python package
BLOCKMODULE=`echo $1 | tr '[:upper:]' '[:lower:]' | tr '[-.]' '[__]'`
BLOCKNAME=`echo $1 | tr '[:upper:]' '[:lower:]'`

BDIR=$BLOXPATH/${BLOCKMODULE}__1_0
if [ -d $BDIR ]; then
  echo "Skipping creation of " $BDIR " - it already exists."
else
  mkdir $BDIR
  touch $BDIR/__init__.py
  touch $BDIR/e_$BLOCKMODULE.py
fi

DRIVERDIR=$ENGAGE/datablox/drivers/${BLOCKMODULE}__1_0
if [ -d $DRIVERDIR ]; then
  echo "Nothing to do: driver directly $DRIVERDIR already exists"
else
  mkdir $DRIVERDIR
  touch $DRIVERDIR/__init__.py
  cp $ENGAGE/block_driver_template.py $DRIVERDIR/driver.py
  sed "s/BLOCKNAME/$BLOCKNAME/g" <$ENGAGE/block_resource.json.tmpl | sed "s/BLOCKDIR/${BLOCKMODULE}__1_0/g" >$DRIVERDIR/resources.json
  sed "s/BLOCKNAME/$BLOCKNAME/g" <$ENGAGE/block_packages.json.tmpl | sed "s/BLOCKDIR/${BLOCKMODULE}__1_0/g" >$DRIVERDIR/packages.json
  echo "Created engage driver at $DRIVERDIR"
fi

echo "Block creation for $BLOCKNAME successful."
exit 0
