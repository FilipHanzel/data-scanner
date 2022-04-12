export PYTHONDONTWRITEBYTECODE=1
DIR=$(dirname "${BASH_SOURCE[0]}")
(cd $DIR ; python -m unittest tests)
