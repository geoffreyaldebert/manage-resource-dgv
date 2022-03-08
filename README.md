Need a working udata, udata-search-service

```
docker-compose up -d
./init_package.sh
pip install -r requirements.txt
# one tab
python app.py
# second tab
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
rq worker -c settings
```
