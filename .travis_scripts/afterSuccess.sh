#! /bin/bash

COL="metrics"
LOAD=`uptime | sed -e 's/^.*:[ \t]*//;s/,//g'`
UPA=`echo "$LOAD" | cut -d ' ' -f 1`
UPB=`echo "$LOAD" | cut -d ' ' -f 2`
UPC=`echo "$LOAD" | cut -d ' ' -f 3`
MEM=`find . -type f -name 'metrics.out' -exec cat {} \;`

KEY="$TRAVIS_JOB_ID-$TRAVIS_JOB_NUMBER."`date '+%s'`
JSON=`grep '<testsuite ' */target/test-reports/*.xml | perl -pe 's/^.+name="//;s/".+time="/":"/;s/^/"/;s/>$//' | awk 'BEGIN { printf(","); i = 0; } { if (i > 0) printf(","); i = i + 1; printf($0); } END { printf("}\n") }' | sed -e "s/^,/{\"version\":\"0.11\",\"freeMem\":$MEM,\"load1m\":$UPA,\"load5m\":$UPB,\"load15m\":$UPC,\"akka\":\"$AKKA_VERSION\",\"dbprofile\":\"$MONGO_PROFILE\",\"mongodb\":\"$MONGO_VER\",/"`

cat > /dev/stdout <<EOF
-----
# Push to the metrics collection
#
# key: $KEY
-----

$JSON

-----
EOF

curl -i "https://api.orchestrate.io/v0/$COL/$KEY" \
  -XPUT \
  -u "$ORCHESTRATE_AUTH:" \
  -H 'If-None-Match: "*"' \
  -H "Content-Type: application/json" \
  -d "$JSON"
