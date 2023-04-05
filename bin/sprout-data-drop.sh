#!/usr/bin/env bash

# Rscript ./schema/curated/file_transfer_script.R

/usr/bin/expect << EOD
 spawn /usr/bin/sftp dcyf-poc-sprout@mft.wa.gov:Sprout
 set timeout -1
 expect "Enter password for dcyf-poc-sprout"
 send "$MFTPASS\r"
 expect "sftp>"
 send "put ./bin/testfile.txt"
 expect "sftp>"
 send "bye\r"
EOD

# if [ -d "./sprout-copies" ]; then rm -Rf "./sprout-copies"; fi
