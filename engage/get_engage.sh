#!/bin/bash
# This shell script will get an engage distribution from either github or a
# local git repository and build it. This needs to be run from
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

if [ -d $ENGAGE_DIST ]; then
  echo "Removing old distribution at $ENGAGE_DIST"
  run rm -rf $ENGAGE_DIST
fi

ENGAGE_REPO_DIR=`cd ../..; pwd`/engage
echo "[get_engage.sh] Looking for engage internal src at $ENGAGE_REPO_DIR"

if [ -d $ENGAGE_REPO_DIR ]; then
  if [[ "$ENGAGE_GITHUB_BRANCH" == "" ]]; then
    ENGAGE_REPO_DIR=`cd ../../engage; pwd`
    #ENGAGE_BUILD=$ENGAGE_REPO_DIR/build_output/engage
    echo "[get_engage.sh] Using Engage source at $ENGAGE_REPO_DIR"
    # run rm -rf $ENGAGE_DIST
    # if ! [ -d $ENGAGE_BUILD ]; then
    #   cd $ENGAGE_REPO_DIR
    #   echo "[get_engage.sh] Building engage"
    #   run make
    # fi
    run cp -r $ENGAGE_REPO_DIR $ENGAGE_DIST
    echo "[get_engage.sh] Engage copied to $ENGAGE_DIST"
    #exit 0
  else
    echo "[get_engage.sh] Local engage tree present, but \$ENGAGE_GITHUB_BRANCH set to $ENGAGE_GITHUB_BRANCH, using github"
  fi
else
  ENGAGE_GITHUB_BRANCH=master
fi


# remove old non-git repo, if present
# if [ -d $ENGAGE_DIST ]; then
#   if ! [ -d $ENGAGE_DIST/.git ]; then
#     echo "[get_engage.sh] $ENGAGE_DIST is not a git repository, deleting"
#     run rm -rf $ENGAGE_DIST
#   fi
# fi

if ! [ -d $ENGAGE_DIST ]; then
  echo "Getting engage from github, branch $ENGAGE_GITHUB_BRANCH"
  run git clone git://github.com/genforma/engage.git engage-dist
  cd $ENGAGE_DIST
  run git pull origin $ENGAGE_GITHUB_BRANCH
  run git checkout $ENGAGE_GITHUB_BRANCH
fi

cd $ENGAGE_DIST
echo "[get_engage.sh] Building engage"
run make clean
run make
run rm -rf .git test_data
echo "[get_engage.sh] Engage built successfully"
exit 0
