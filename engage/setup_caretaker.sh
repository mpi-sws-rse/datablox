#!/bin/bash
# This is use to setup the caretaker on worker nodes
INSTALL_DIR=~/apps

function remove_old_install {
  echo "Removing old datablox install"
  cd $INSTALL_DIR/engage/bin
  ./svcctl -p $INSTALL_DIR/config/master.pw stop
  rm -rf $INSTALL_DIR.prev
  mv  $INSTALL_DIR $INSTALL_DIR.prev
}

if [ -d $INSTALL_DIR ]; then
  CAN_REUSE_INSTALL="no"
  if [[ "$1" == "--reuse-existing-install" ]]; then
    if [ -x $INSTALL_DIR/engage/bin/svcctl ]; then
      cd $INSTALL_DIR/engage/bin
      ./svcctl -p $INSTALL_DIR/config/master.pw start caretaker
      rc=$?
      if [[ "$rc" == "0" ]]; then
        CAN_REUSE_INSTALL="yes"
      fi
    fi
  fi
  if [[ "$CAN_REUSE_INSTALL" == "yes" ]]; then
    echo "Reusing existing datablox install"
    exit 0
  else
    remove_old_install
  fi
fi

echo "Bootstrapping engage deployment home"
cd ~
rm -rf ./engage
tar xzf engage-dist.tar.gz
cd engage
./bootstrap.py $INSTALL_DIR
rc=$?
if [[ "$rc" != "0" ]]; then
  echo "Bootstrap failed"
  exit $rc
fi

# create a random password
python -c "import random; import string; print ''.join([random.choice(string.letters+string.digits) for i in range(10)])" >~/pw
chmod 600 ~/pw

echo "Running deployment"
cd $INSTALL_DIR/engage/bin
./deployer -p ~/pw $INSTALL_DIR/engage/metadata/datablox/caretaker_install_spec.json
rc=$?
if [[ "$rc" != "0" ]]; then
  echo "Caretaker deployment failed"
  exit $rc
fi

rm ~/pw
echo "Caretaker deployment successful"
exit 0






  