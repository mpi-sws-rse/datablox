#!/bin/bash
# This shell script will get an engage distribution from either github or a
# local genForma internal repository and build it. This needs to be run from
# blox/engage. If you want to use a specific branch in the github repository,
# or force the script to always use github, set the environment variable
# ENGAGE_GITHUB_BRANCH to your desired branch name (e.g. master).

ENGAGE_DIST=`pwd`/engage-dist

function run {
  echo " [get_engage.sh] " $*
  if ! $*; then
    echo " [get_engage.sh] $1 failed, exiting"
    exit 1
  fi
}

TEST_ENGAGE_SRC=`cd ../..; pwd`/code/src/engage
echo "[get_engage.sh] Looking for engage internal src at $TEST_ENGAGE_SRC"

if [ -d $TEST_ENGAGE_SRC ]; then
  if [[ "$ENGAGE_GITHUB_BRANCH" == "" ]]; then
    ENGAGE_CODE=`cd ../../code; pwd`
    ENGAGE_BUILD=$ENGAGE_CODE/build_output/engage
    echo "[get_engage.sh] Using Engage source at $ENGAGE_CODE"
    run rm -rf $ENGAGE_DIST
    cd $ENGAGE_CODE
    echo "[get_engage.sh] Building engage"
    run make engage
    echo cp -r $ENGAGE_BUILD $ENGAGE_DIST
    run cp -r $ENGAGE_BUILD $ENGAGE_DIST
    echo "[get_engage.sh] Engage built successfully"
    exit 0
  else
    echo "[get_engage.sh] Local engage tree present, but \$ENGAGE_GITHUB_BRANCH set to $ENGAGE_GITHUB_BRANCH, using github"
  fi
else
  ENGAGE_GITHUB_BRANCH=master
fi

echo "Getting engage from github, branch $ENGAGE_GITHUB_BRANCH"

# remove old non-git repo, if present
if [ -d $ENGAGE_DIST ]; then
  if ! [ -d $ENGAGE_DIST/.git ]; then
    echo "[get_engage.sh] $ENGAGE_DIST is not a git repository, deleting"
    run rm -rf $ENGAGE_DIST
  fi
fi

if ! [ -d $ENGAGE_DIST ]; then
  run git clone git://github.com/genforma/engage.git engage-dist
fi
cd $ENGAGE_DIST
run git query origin $ENGAGE_GITHUB_BRANCH
run git checkout $ENGAGE_GITHUB_BRANCH

cd $ENGAGE_DIST
echo "[get_engage.sh] Building engage"
run make clean
run make
echo "[get_engage.sh] Engage built successfully"
exit 0
