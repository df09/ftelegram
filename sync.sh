#!/bin/bash

# git push/pull
git add . && git commit -m 'sync: upd' && git push
ssh isushkov@files.shellpea.com 'bash -c "cd ftelegram && git pull"'
# sync offsets
if [[ "$1" == "offsets" ]]; then
  scp filters/work-offsets.yml isushkov@files.shellpea.com:/home/isushkov/ftelegram/filters/work-offsets.yml
fi
# restart cron
ssh root@files.shellpea.com 'bash -c "echo RESTART_CRON_SERVICE"'
ssh root@files.shellpea.com 'bash -c "systemctl restart cron.service"'
ssh root@files.shellpea.com 'bash -c "echo exit-status: $?"'
