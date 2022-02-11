Need a working udata, udata-search-service

```
docker-compose up -d
./init_package.sh
pip install -r requirements.txt
# one tab
python app.py
# second tab
rq worker -c settings
```
