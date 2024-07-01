#!/bin/bash
# syn filters with server

git add . && git commit -m 'sync: upd' && git push
ssh isushkov@files.shellpea.com 'bash -c "cd ftelegram && git pull"'
ssh root@files.shellpea.com 'bash -c "echo RESTART_CRON_SERVICE start"'
ssh root@files.shellpea.com 'bash -c "systemctl restart cron.service"'
ssh root@files.shellpea.com 'bash -c "echo RESTART_CRON_SERVICE done"'
