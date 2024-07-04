cd python && python3 setup.py generate_flex_sdk
python3 setup.py bdist_wheel && pip3 install install ./dist/*
cd ../ && make gsctl
