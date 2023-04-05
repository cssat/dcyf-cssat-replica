#!/usr/bin/env bash

Rscript ./schema/curated/file_transfer_script.R

/usr/bin/expect << EOD
spawn /usr/bin/sftp dcyf-poc-sprout@mft.wa.gov:Sprout
set timeout -1
expect "*Password:*"
sleep 1
send "$MFTPASS\r"
expect "sftp>"
send "mput ./sprout-copies/*.csv\r"
expect "sftp>"
send "bye\r"
EOD

if [ -d "./sprout-copies" ]; then rm -Rf "./sprout-copies"; fi
