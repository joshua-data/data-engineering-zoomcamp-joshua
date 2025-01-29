set -e

  TAXI_TYPE=$1 # "yellow", "green"
  YEAR=$2 # 2024

URL_PREFIX="https://d37ci6vzurychx.cloudfront.net/trip-data"

# YEAR 2024: MONTH 1 - 11
if [ "$YEAR" -eq 2024 ]; then
  MONTHS=$(seq 1 11)
# YEAR 2023: MONTH 1 - 12
elif [ "$YEAR" -eq 2023 ]; then
  MONTHS=$(seq 1 12)
else
  echo "Enter 2023 or 2024 only for the Year."
  exit 1
fi

for MONTH in $MONTHS; do

  MONTH_FMT=`printf "%02d" ${MONTH}`

  URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${MONTH_FMT}.parquet"

  FOLDERPATH="data/raw/${TAXI_TYPE}/${YEAR}/${MONTH_FMT}"
  FNAME="${TAXI_TYPE}_tripdata_${YEAR}_${MONTH_FMT}.parquet"
  FPATH="${FOLDERPATH}/${FNAME}"

  echo "Downloading ${URL} to ${FPATH}"
  mkdir -p ${FOLDERPATH}
  wget ${URL} -O ${FPATH}

done