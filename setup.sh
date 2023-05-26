SRCNAME=light_pipe
TESTDIR=tests

### Run unit tests ###
python3 -m unittest discover ${TESTDIR} -v

### Update pip ###
python3 -m pip install --upgrade pip

### Make docs ###
python3 -m pip install --upgrade pdoc3
pdoc ${SRCNAME} -o ./docs/ --html --force
mv ./docs/${SRCNAME}/* ./docs/
rm -r ./docs/${SRCNAME}

### Build dist from source ###
python3 -m pip install --upgrade build
python3 -m build

### Upload dist to PyPI ###
python3 -m pip install --upgrade twine
python3 -m twine upload dist/*