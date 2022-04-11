DIR=$(dirname "${BASH_SOURCE[0]}")
(cd $DIR ; python -m unittest tests)
