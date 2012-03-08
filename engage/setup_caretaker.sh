#!/bin/bash
# This is use to setup the caretaker on worker nodes


if [ -d ~/apps ]; then
    echo "Removing old DJM install"
    cd ~/apps/engage/bin
    ./svcctl -p ~/apps/config/master.pw stop
    cd ~
    rm -rf ./apps.prev
    mv ./apps apps.prev
fi

echo "Bootstrapping engage deployment home"
cd ~
rm -rf ./engage
tar xzf engage-dist.tar.gz
cd engage
./bootstrap.py ~/apps
rc=$?
if [[ "$rc" != "0" ]]; then
  echo "Bootstrap failed"
  exit $rc
fi

# create a random password
python -c "import random; import string; print ''.join([random.choice(string.letters+string.digits) for i in range(10)])" >~/pw
chmod 600 ~/pw

echo "Running deployment"
cd ~/apps/engage/bin
./deployer -p ~/pw ~/apps/engage/metadata/datablox/caretaker_install_spec.json
rc=$?
if [[ "$rc" != "0" ]]; then
  echo "Caretaker deployment failed"
  exit $rc
fi

rm ~/pw
echo "Caretaker deployment successful"
exit 0






  