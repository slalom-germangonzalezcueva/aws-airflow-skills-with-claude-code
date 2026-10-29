#!/bin/bash
access_key_var=AWS_ACCESS_KEY_ID_$AppEnv
secret_key_var=AWS_SECRET_ACCESS_KEY_$AppEnv
default_region_var=AWS_DEFAULT_REGION_$AppEnv
backup_region_var=AWS_BACKUP_REGION_$AppEnv
export AWS_ACCESS_KEY_ID=${!access_key_var}
export AWS_SECRET_ACCESS_KEY=${!secret_key_var}
if [[ $region == ${!backup_region_var} ]]; then
  export AWS_DEFAULT_REGION=$region
else
  export AWS_DEFAULT_REGION=${!default_region_var}
fi
